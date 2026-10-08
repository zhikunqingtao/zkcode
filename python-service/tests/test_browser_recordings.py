"""Private recording spool: no side effects are replayed for an ACK."""
import pytest


def test_sealed_recording_is_owner_bound_and_ack_is_idempotent(tmp_path):
    from services.browser_recordings import RecordingSpool
    spool = RecordingSpool(tmp_path / 'spool')
    identity = {'batch_id': '00000000-0000-4000-8000-000000000001', 'session_id': 'session', 'run_id': 'run', 'invocation_id': 'invocation'}
    batch = spool.reserve(identity, 'generation', {'har': True})
    (batch / 'network.har').write_text('{"log":{}}')
    manifest = spool.seal(identity['batch_id'], identity, 'generation')
    assert manifest['files'][0]['status'] == 'available'
    assert spool.ack(identity['batch_id'], identity, manifest['manifest_sha256'])
    assert spool.ack(identity['batch_id'], identity, manifest['manifest_sha256'])
    assert not (batch / 'network.har').exists()
    with pytest.raises(ValueError, match='OWNER'):
        spool.ack(identity['batch_id'], {**identity, 'run_id': 'wrong'}, manifest['manifest_sha256'])


def test_spool_rejects_symlink(tmp_path):
    from services.browser_recordings import RecordingSpool
    outside = tmp_path / 'outside'; outside.mkdir()
    root = tmp_path / 'link'; root.symlink_to(outside, target_is_directory=True)
    with pytest.raises((ValueError, OSError)):
        RecordingSpool(root).reserve({'batch_id': '00000000-0000-4000-8000-000000000001'}, 'generation', {})


def test_recording_byte_budget_is_explicit_and_can_be_consumed(tmp_path):
    from services.browser_recordings import RecordingSpool, MAX_FILE_BYTES
    spool = RecordingSpool(tmp_path / 'spool')
    identity = {'batch_id': '00000000-0000-4000-8000-000000000002', 'session_id': 'session', 'run_id': 'run', 'invocation_id': 'invocation'}
    batch = spool.reserve(identity, 'generation', {'har': True})
    with (batch / 'network.har').open('wb') as file:
        file.truncate(MAX_FILE_BYTES + 1)
    manifest = spool.seal(identity['batch_id'], identity, 'generation')
    assert manifest['files'][0]['status'] == 'omitted_budget'
    assert 'sha256' not in manifest['files'][0]
    assert spool.ack(identity['batch_id'], identity, manifest['manifest_sha256'])
    assert not (batch / 'network.har').exists()


@pytest.mark.asyncio
async def test_real_chromium_recording_seals_after_close_and_consumes_once(tmp_path, monkeypatch):
    import uuid
    from routers import browser, journey
    from services.browser_service import BrowserService
    from services.browser_recordings import RecordingSpool
    from services.journey_models import JourneyRunRequest
    from services.browser_models import CloseSessionRequest, RecordingAckRequest
    service = BrowserService()
    service.recordings = RecordingSpool(tmp_path / 'spool')
    monkeypatch.setattr(browser, 'browser_service', service)
    identity = {'batch_id': str(uuid.uuid4()), 'session_id': 'fixture', 'run_id': 'run', 'invocation_id': 'invocation'}
    await service.startup()
    try:
        result = await journey.journey_run(JourneyRunRequest(session_id='record-fixture', base_url='http://127.0.0.1', steps=[{'action':'screenshot'}], record={'trace': True, 'har': True, 'video': True}, recording=identity))
        assert result.passed
        closed = await browser.close_session(CloseSessionRequest(session_id='record-fixture', recording=identity))
        assert closed.success and closed.data['closed']
        manifest = closed.data['recording_manifest']
        assert {item['kind'] for item in manifest['files']} == {'trace', 'har', 'video'}
        assert all(item['status'] == 'available' for item in manifest['files'])
        ack = RecordingAckRequest(identity=identity, manifest_sha256=manifest['manifest_sha256'])
        assert (await browser.acknowledge_recordings(ack)).success
        assert (await browser.acknowledge_recordings(ack)).success
    finally:
        await service.shutdown()


def test_unacknowledged_capacity_blocks_new_recordings_and_age_alone_never_consumes(tmp_path):
    import json
    import time
    import uuid
    from services.browser_recordings import RecordingSpool
    spool = RecordingSpool(tmp_path / 'spool')
    identities = []
    for _ in range(10):
        identity = {'batch_id':str(uuid.uuid4()),'session_id':'session','run_id':'run','invocation_id':'invocation'}
        spool.reserve(identity, 'generation', {'har':True})
        identities.append(identity)
    with pytest.raises(ValueError, match='CAPACITY'):
        spool.reserve({'batch_id':str(uuid.uuid4()),'session_id':'session','run_id':'run','invocation_id':'invocation'},'generation',{'har':True})
    first = identities[0]
    batch = spool.root / first['batch_id']
    owner = json.loads((batch / 'owner.json').read_text())
    owner['created_at'] = time.time() - 90000
    (batch / 'owner.json').write_text(json.dumps(owner))
    assert first['batch_id'] in spool.orphan_candidates()
    assert batch.exists(), 'scan alone never deletes unacknowledged contents'
    manifest = spool.seal(first['batch_id'], first, 'generation')
    assert manifest['files'][0]['status'] == 'missing'
    spool.ack(first['batch_id'], first, manifest['manifest_sha256'])
    assert spool.prune_unreferenced(first['batch_id'])
    assert not batch.exists()

@pytest.mark.asyncio
async def test_not_created_proof_requires_context_creation_and_batch_absent(tmp_path, monkeypatch):
    import uuid
    from routers import browser
    from services.browser_service import BrowserService
    from services.browser_recordings import RecordingSpool
    from services.browser_models import CloseSessionRequest
    service = BrowserService()
    service.recordings = RecordingSpool(tmp_path / 'spool')
    monkeypatch.setattr(browser, 'browser_service', service)
    identity = {'batch_id':str(uuid.uuid4()),'session_id':'s','run_id':'r','invocation_id':'i'}
    result = await browser.close_session(CloseSessionRequest(session_id='never-created', recording=identity))
    assert result.success
    assert result.data['recording_finalization'] == {'phase':'not_created','identity':identity}
    service.recordings.reserve(identity, 'old-generation', {'har':True})
    result = await browser.close_session(CloseSessionRequest(session_id='never-created', recording=identity))
    assert result.success
    assert 'recording_finalization' not in result.data
    assert result.data['recording_manifest']['files'][0]['status'] == 'missing'
