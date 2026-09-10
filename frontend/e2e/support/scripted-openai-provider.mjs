import http from 'node:http';

const port = Number.parseInt(process.env.ZK_E2E_PROVIDER_PORT ?? '', 10);
if (!Number.isInteger(port) || port < 1024 || port > 65535) {
  throw new Error('ZK_E2E_PROVIDER_PORT must be an isolated non-privileged port');
}

const answer = '来自脚本 Provider 的持久化回复';

function writeJson(response, status, payload) {
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  response.end(JSON.stringify(payload));
}

const server = http.createServer((request, response) => {
  if (request.method === 'GET' && request.url === '/health') {
    writeJson(response, 200, { status: 'ok' });
    return;
  }
  if (request.method !== 'POST' || request.url !== '/v1/chat/completions') {
    writeJson(response, 404, { error: { message: 'fixture route not found' } });
    return;
  }

  const chunks = [];
  request.on('data', chunk => chunks.push(chunk));
  request.on('end', () => {
    let body;
    try {
      body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      writeJson(response, 400, { error: { message: 'invalid JSON request' } });
      return;
    }
    if (body.model !== 'qwen3.8-max-0902' || body.stream !== true
        || !Array.isArray(body.messages)) {
      writeJson(response, 422, { error: { message: 'unexpected fixture request shape' } });
      return;
    }

    response.writeHead(200, {
      'cache-control': 'no-cache',
      'content-type': 'text/event-stream; charset=utf-8',
      connection: 'keep-alive',
    });
    response.write(`data: ${JSON.stringify({
      choices: [{ delta: { content: answer }, finish_reason: null }],
    })}\n\n`);
    response.write(`data: ${JSON.stringify({
      choices: [{ delta: {}, finish_reason: 'stop' }],
      usage: { prompt_tokens: 13, completion_tokens: 8 },
    })}\n\n`);
    response.end('data: [DONE]\n\n');
  });
});

server.listen(port, '127.0.0.1');

function shutdown() {
  server.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
