// Real Rust -> managed Python UDS -> actual parsers, in a disposable workspace.
import { spawn, execFile } from 'node:child_process';
import { mkdtemp, mkdir, writeFile, rm, access } from 'node:fs/promises';
import { constants, createReadStream } from 'node:fs';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';

const repository = fileURLToPath(new URL('../../../', import.meta.url));
const binary = process.env.ZK_E2E_SERVER_BINARY;
if (!binary) throw new Error('ZK_E2E_SERVER_BINARY must name an explicitly built Rust server');
await access(binary, constants.X_OK);
async function binaryHash() {
    const hash = createHash('sha256');
    for await (const chunk of createReadStream(binary)) hash.update(chunk);
    return hash.digest('hex');
}
const beforeHash = await binaryHash();
console.log(`Analysis E2E Rust binary: ${binary}; before SHA256=${beforeHash}`);
const fixture = await mkdtemp('/tmp/zk-analysis-e2e-');
const workspace = path.join(fixture, 'workspace');
await mkdir(workspace);
await writeFile(path.join(workspace, 'api.py'), `from fastapi import APIRouter
router = APIRouter()
@router.get("/users")
def get_user():
    if valid():
        return fetch()
    return {}
def valid():
    return True
def fetch():
    return {"id": 1}
`);
// Only the disposable workspace is committed; never the user's repository.
const execute = promisify(execFile);
const git = (...args) => execute('git', ['-C', workspace, '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgSign=false', ...args], {
    timeout: 10_000, env: { ...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null' },
});
await git('init', '--quiet');
await git('config', 'user.name', 'Analysis E2E');
await git('config', 'user.email', 'fixture@example.invalid');
await git('add', '--', 'api.py');
await git('commit', '--quiet', '-m', 'feat: native analysis fixture');
const child = spawn(binary, [], {
    cwd: workspace,
    stdio: 'inherit',
    env: {
        ...process.env,
        ZK_HOST: '127.0.0.1', ZK_PORT: process.env.ZK_E2E_SERVER_PORT,
        ZK_DB_PATH: path.join(fixture, 'runtime.db'),
        ZK_WORKSPACE_DEFAULT_ROOT: workspace, ZK_WORKSPACE_ALLOWED_ROOTS: workspace,
        ZK_CORS_ALLOWED_ORIGINS: `http://127.0.0.1:${process.env.ZK_E2E_FRONTEND_PORT}`,
        ZK_PYTHON_ENABLED: 'true', ZK_PYTHON_UDS: path.join(fixture, 'p.sock'),
        ZK_PYTHON_SERVICE_DIR: path.join(repository, 'python-service'),
        ZK_PYTHON_CMD: path.join(repository, 'python-service/.venv/bin/python'),
        ZK_STATIC_DIR: path.join(repository, 'crates/zk-server/resources/static'),
        ZK_SNAPSHOT_DIR: path.join(fixture, 'snapshots'),
        ZK_SCRATCHPAD_SYSTEM_ROOT: path.join(fixture, 'scratchpad'),
        MCP_REGISTRY_PATH: path.join(fixture, 'mcp.json'),
        ZK_LOG_FILE: path.join(fixture, 'server.jsonl'), ZK_LOG: 'warn',
        ZK_DEFAULT_MODEL: 'qwen3.8-max-0902', ZK_LLM_API_KEY: 'unused-static-analysis-fixture',
        ZK_DEMO_CREDENTIAL_DB: path.join(repository, 'configuration/bootstrap/demo-credentials.db'),
        ZK_DEV_ALLOW_DEMO_CREDENTIAL: '0', ZK_LLM_BASE_URL: 'http://127.0.0.1:1/v1',
        ZK_AGENT_ENABLED: 'false', ZK_AGENT_WRITE_ENABLED: 'false',
        ZK_CRON_ENABLED: 'false', ZK_SWARM_ENABLED: 'false', ZK_WORKTREE_ENABLED: 'false',
        ZK_AUTO_RESUME_SAFE_TASKS: 'false', ZK_SHARED_WORKSPACE_ENABLED: 'false',
    },
});
let stopping = false;
function stop() {
    if (stopping) return;
    stopping = true;
    child.kill('SIGTERM');
    // Allow the Rust supervisor's 10-second Python shutdown grace to finish.
    const timeout = setTimeout(() => child.kill('SIGKILL'), 20_000);
    timeout.unref();
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
child.on('error', error => { console.error(error); stop(); });
child.on('close', async code => {
    const afterHash = await binaryHash();
    console.log(`Analysis E2E Rust binary after SHA256=${afterHash}; unchanged=${beforeHash === afterHash}`);
    await rm(fixture, { recursive: true, force: true });
    process.exit(beforeHash !== afterHash ? 1 : stopping ? 0 : code ?? 1);
});
