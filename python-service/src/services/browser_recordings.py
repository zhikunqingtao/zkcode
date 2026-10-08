"""Host-owned private recording batches, sealed once and consumed only by ACK.

Paths in this protocol are relative to a configured root, never arbitrary tempfile
paths. Acknowledgement tombstones are small, durable and survive sidecar restart.
"""
import hashlib
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid
from contextlib import contextmanager

_BATCH = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_BATCH_BYTES = 20 * 1024 * 1024


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def _read_json(fd, name):
    child = os.open(name, _FLAGS, dir_fd=fd)
    try:
        info = os.fstat(child)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
            raise ValueError('RECORDING_MANIFEST_INVALID')
        with os.fdopen(child, 'rb', closefd=False) as source:
            return json.loads(source.read(65537))
    finally:
        os.close(child)


def _write_json(fd, name, value):
    data = _encoded(value)
    if len(data) > 65536:
        raise ValueError('RECORDING_MANIFEST_LIMIT')
    temp = name + '.pending-' + uuid.uuid4().hex
    child = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
    try:
        with os.fdopen(child, 'wb', closefd=False) as target:
            target.write(data); target.flush(); os.fsync(child)
        os.rename(temp, name, src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
    finally:
        os.close(child)
        try:
            os.unlink(temp, dir_fd=fd)
        except FileNotFoundError:
            pass


@contextmanager
def _exclusive(fd):
    lock = os.open('.spool.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd)
    try:
        if not stat.S_ISREG(os.fstat(lock).st_mode):
            raise ValueError('RECORDING_LOCK_INVALID')
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield
    finally:
        os.close(lock)


class RecordingSpool:
    def __init__(self, root=None):
        self.root = Path(root or os.environ.get('ZK_BROWSER_RECORDING_SPOOL', Path.home() / '.zkcode' / 'browser-recordings'))

    @contextmanager
    def _root(self):
        self.root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            self.root.mkdir(mode=0o700)
        except FileExistsError:
            pass
        fd = os.open(self.root, _FLAGS | os.O_DIRECTORY)
        try:
            info = os.fstat(fd)
            if info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError('RECORDING_SPOOL_NOT_PRIVATE')
            yield fd
        finally:
            os.close(fd)

    @contextmanager
    def _batch(self, batch_id):
        if not _BATCH.fullmatch(batch_id):
            raise ValueError('RECORDING_BATCH_INVALID')
        with self._root() as root:
            fd = os.open(batch_id, _FLAGS | os.O_DIRECTORY, dir_fd=root)
            try:
                yield fd
            finally:
                os.close(fd)

    def absent(self, batch_id):
        """Only ENOENT proves absence; permission, symlink and I/O errors do not."""
        if not _BATCH.fullmatch(batch_id):
            raise ValueError('RECORDING_BATCH_INVALID')
        with self._root() as root, _exclusive(root):
            try:
                os.stat(batch_id, dir_fd=root, follow_symlinks=False)
            except FileNotFoundError:
                return True
            return False

    def reserve(self, identity, generation, requested):
        batch_id = identity.get('batch_id', '')
        if not _BATCH.fullmatch(batch_id) or any(not isinstance(identity.get(key), str) or not identity[key] for key in ('session_id', 'run_id', 'invocation_id')):
            raise ValueError('RECORDING_OWNER_INVALID')
        with self._root() as root, _exclusive(root):
            pending = 0
            for name in os.listdir(root):
                if not _BATCH.fullmatch(name):
                    continue
                with self._batch(name) as existing:
                    try:
                        _read_json(existing, 'ack.json')
                    except FileNotFoundError:
                        pending += 1
            if pending >= 10:
                raise ValueError('RECORDING_FINALIZATION_CAPACITY_REACHED')
            os.mkdir(batch_id, 0o700, dir_fd=root)
        with self._batch(batch_id) as fd:
            _write_json(fd, 'owner.json', {'version': 1, 'identity': identity, 'generation': generation, 'requested': requested, 'created_at': time.time()})
            if requested.get('video'):
                os.mkdir('video', 0o700, dir_fd=fd)
        return self.root / batch_id

    def seal(self, batch_id, identity, generation):
        with self._batch(batch_id) as fd, _exclusive(fd):
            owner = _read_json(fd, 'owner.json')
            if owner['identity'] != identity or (generation is not None and owner['generation'] != generation):
                raise ValueError('RECORDING_OWNER_MISMATCH')
            try:
                manifest = _read_json(fd, 'manifest.json')
                return {**manifest, 'manifest_sha256': hashlib.sha256(_encoded(manifest)).hexdigest()}
            except FileNotFoundError:
                pass
            files = []
            remaining = MAX_BATCH_BYTES
            requested = owner['requested']
            candidates = [('network.har', 'har'), ('trace.zip', 'trace')]
            if requested.get('video'):
                video = os.open('video', _FLAGS | os.O_DIRECTORY, dir_fd=fd)
                try:
                    candidates += [('video/' + name, 'video') for name in sorted(os.listdir(video)) if re.fullmatch(r'[A-Za-z0-9_-]+\.webm', name)]
                finally:
                    os.close(video)
            if len(candidates) > 50:
                raise ValueError('RECORDING_FILE_COUNT_EXCEEDED')
            for path, kind in candidates:
                if not requested.get(kind):
                    continue
                try:
                    parent = fd
                    if path.startswith('video/'):
                        parent = os.open('video', _FLAGS | os.O_DIRECTORY, dir_fd=fd)
                    try:
                        child = os.open(path.split('/')[-1], _FLAGS, dir_fd=parent)
                    finally:
                        if parent != fd:
                            os.close(parent)
                except FileNotFoundError:
                    files.append({'path': path, 'kind': kind, 'status': 'missing', 'error_code': 'RECORDING_NOT_CREATED'})
                    continue
                try:
                    info = os.fstat(child)
                    if not stat.S_ISREG(info.st_mode):
                        raise ValueError('RECORDING_FILE_INVALID')
                    os.fchmod(child, 0o600)
                    item = {'path': path, 'kind': kind, 'size': info.st_size, 'device': info.st_dev, 'inode': info.st_ino, 'mtime_ns': info.st_mtime_ns}
                    if info.st_size > MAX_FILE_BYTES or info.st_size > remaining:
                        item.update(status='omitted_budget', error_code='RECORDING_BYTE_BUDGET_EXCEEDED')
                    else:
                        digest = hashlib.sha256()
                        read_bytes = 0
                        while block := os.read(child, min(65536, MAX_FILE_BYTES + 1 - read_bytes)):
                            read_bytes += len(block)
                            if read_bytes > MAX_FILE_BYTES:
                                raise ValueError('RECORDING_CHANGED_DURING_SEAL')
                            digest.update(block)
                        after = os.fstat(child)
                        if (after.st_size, after.st_mtime_ns) != (info.st_size, info.st_mtime_ns):
                            raise ValueError('RECORDING_CHANGED_DURING_SEAL')
                        item.update(status='available', sha256=digest.hexdigest())
                        remaining -= info.st_size
                    files.append(item)
                finally:
                    os.close(child)
            if requested.get('video') and not any(item['kind'] == 'video' for item in files):
                files.append({'path': None, 'kind': 'video', 'status': 'missing', 'error_code': 'RECORDING_NOT_CREATED'})
            manifest = {'version': 1, 'batch_id': batch_id, 'identity': identity, 'generation': owner['generation'], 'files': files}
            _write_json(fd, 'manifest.json', manifest)
            return {**manifest, 'manifest_sha256': hashlib.sha256(_encoded(manifest)).hexdigest()}

    def ack(self, batch_id, identity, manifest_sha256):
        with self._batch(batch_id) as fd, _exclusive(fd):
            owner = _read_json(fd, 'owner.json')
            if owner['identity'] != identity:
                raise ValueError('RECORDING_OWNER_MISMATCH')
            manifest = _read_json(fd, 'manifest.json')
            if hashlib.sha256(_encoded(manifest)).hexdigest() != manifest_sha256:
                raise ValueError('RECORDING_MANIFEST_MISMATCH')
            # A lost HTTP response may retry the exact committed acknowledgement.
            try:
                ack = _read_json(fd, 'ack.json')
                if ack['manifest_sha256'] != manifest_sha256:
                    raise ValueError('RECORDING_MANIFEST_MISMATCH')
                return True
            except FileNotFoundError:
                pass
            for item in manifest['files']:
                path = item.get('path')
                if not path or item['status'] == 'missing':
                    continue
                parent = fd
                if path.startswith('video/'):
                    parent = os.open('video', _FLAGS | os.O_DIRECTORY, dir_fd=fd)
                try:
                    name = path.split('/')[-1]
                    try:
                        info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    except FileNotFoundError:
                        continue  # Previous ACK deleted this item before interruption.
                    if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) != (item['device'], item['inode'], item['size'], item['mtime_ns']):
                        raise ValueError('RECORDING_CHANGED_AFTER_SEAL')
                    os.unlink(name, dir_fd=parent)
                finally:
                    if parent != fd:
                        os.close(parent)
            _write_json(fd, 'ack.json', {'manifest_sha256': manifest_sha256, 'acknowledged_at': time.time()})
            return True

    def orphan_candidates(self):
        """Only age-qualified IDs; Rust must prove absence of durable protection."""
        candidates = []
        with self._root() as root:
            for name in sorted(os.listdir(root)):
                if not _BATCH.fullmatch(name):
                    continue
                with self._batch(name) as fd:
                    try:
                        created = _read_json(fd, 'owner.json')['created_at']
                    except FileNotFoundError:
                        created = os.fstat(fd).st_mtime
                    if time.time() - created >= 86400:
                        candidates.append(name)
                if len(candidates) >= 50:
                    break
        return candidates

    def prune_unreferenced(self, batch_id):
        """Caller must have checked the host resource ledger before this call."""
        import shutil
        if not shutil.rmtree.avoids_symlink_attacks:
            raise ValueError('RECORDING_SAFE_DELETE_UNAVAILABLE')
        try:
            with self._batch(batch_id) as fd:
                try:
                    created = _read_json(fd, 'owner.json')['created_at']
                except FileNotFoundError:
                    created = os.fstat(fd).st_mtime
                if time.time() - created < 86400:
                    raise ValueError('RECORDING_RETENTION_PROTECTED')
            with self._root() as root:
                shutil.rmtree(batch_id, dir_fd=root)
        except FileNotFoundError:
            pass
        return True
