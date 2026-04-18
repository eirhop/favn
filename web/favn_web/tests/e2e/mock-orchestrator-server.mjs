import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';

const PORT = Number(process.env.FAVN_ORCHESTRATOR_PORT ?? 4101);
const HOST = '127.0.0.1';
const VALID_CREDENTIALS = new Map([
	['alice', 'password123'],
	['bob', 'password123']
]);

const RUNS = [
	{ id: 'run_001', status: 'succeeded', target: { type: 'asset', id: 'asset.orders' } },
	{ id: 'run_002', status: 'running', target: { type: 'pipeline', id: 'pipeline.reconcile' } }
];

const RUN_DETAILS = new Map([
	[
		'run_001',
		{
			id: 'run_001',
			status: 'succeeded',
			target: { type: 'asset', id: 'asset.orders' },
			manifest_version_id: 'manifest_v1'
		}
	],
	[
		'run_002',
		{
			id: 'run_002',
			status: 'running',
			target: { type: 'pipeline', id: 'pipeline.reconcile' },
			manifest_version_id: 'manifest_v2'
		}
	]
]);

const MANIFESTS = [
	{ manifest_version_id: 'manifest_v1', status: 'inactive' },
	{ manifest_version_id: 'manifest_v2', status: 'active' }
];

const SCHEDULES = [
	{ schedule_id: 'sched_001', enabled: true, target: { type: 'asset', id: 'asset.orders' } },
	{ schedule_id: 'sched_002', enabled: false, target: { type: 'pipeline', id: 'pipeline.reconcile' } }
];

const SCHEDULE_DETAILS = new Map([
	[
		'sched_001',
		{
			schedule_id: 'sched_001',
			enabled: true,
			cron: '*/5 * * * *',
			target: { type: 'asset', id: 'asset.orders' }
		}
	],
	[
		'sched_002',
		{
			schedule_id: 'sched_002',
			enabled: false,
			cron: '0 * * * *',
			target: { type: 'pipeline', id: 'pipeline.reconcile' }
		}
	]
]);

/** @type {Map<string, { actorId: string; provider: string }>} */
const sessions = new Map();

/** @type {Map<string, { status: number; payload: Record<string, unknown> }>} */
const idempotencyStore = new Map();

function sendJson(response, status, payload) {
	response.writeHead(status, {
		'content-type': 'application/json; charset=utf-8'
	});
	response.end(JSON.stringify(payload));
}

function sendJsonWithHeaders(response, status, payload, headers = {}) {
	response.writeHead(status, {
		'content-type': 'application/json; charset=utf-8',
		...headers
	});
	response.end(JSON.stringify(payload));
}

function sendSse(response, status, eventLines) {
	response.writeHead(status, {
		'content-type': 'text/event-stream; charset=utf-8',
		'cache-control': 'no-cache',
		connection: 'keep-alive'
	});
	response.end(`${eventLines.join('\n')}\n\n`);
}

function authHeaderValid(request) {
	const authorization = request.headers.authorization;
	return typeof authorization === 'string' && authorization.startsWith('Bearer ');
}

function serviceHeaderValid(request) {
	return request.headers['x-favn-service'] === 'favn_web';
}

function requireAuthenticatedSession(request, response) {
	if (!authHeaderValid(request) || !serviceHeaderValid(request)) {
		sendJson(response, 401, {
			error: { message: 'Unauthorized service request' }
		});
		return null;
	}

	const actorId = request.headers['x-favn-actor-id'];
	const sessionId = request.headers['x-favn-session-id'];

	if (typeof actorId !== 'string' || typeof sessionId !== 'string') {
		sendJson(response, 401, {
			error: { message: 'Missing actor/session headers' }
		});
		return null;
	}

	const session = sessions.get(sessionId);
	if (!session || session.actorId !== actorId) {
		sendJson(response, 401, { error: { message: 'Unknown session' } });
		return null;
	}

	return { actorId, sessionId };
}

function withIdempotencyReplay(request, response, session, operationScope, resolveFresh) {
	const idempotencyKey = request.headers['idempotency-key'];

	if (typeof idempotencyKey !== 'string' || idempotencyKey.trim().length === 0) {
		sendJson(response, 422, { error: { message: 'Missing idempotency-key header' } });
		return;
	}

	const storeKey = `${session.actorId}:${operationScope}:${idempotencyKey.trim()}`;
	const replay = idempotencyStore.get(storeKey);

	if (replay) {
		sendJsonWithHeaders(response, replay.status, replay.payload, {
			'x-favn-idempotent-replayed': 'true'
		});
		return;
	}

	const fresh = resolveFresh();
	idempotencyStore.set(storeKey, fresh);
	sendJson(response, fresh.status, fresh.payload);
}

async function readJsonBody(request) {
	const chunks = [];

	for await (const chunk of request) {
		chunks.push(chunk);
	}

	const body = Buffer.concat(chunks).toString('utf8');
	if (!body) return {};

	try {
		return JSON.parse(body);
	} catch {
		return null;
	}
}

function handleLogin(request, response) {
	if (!authHeaderValid(request) || !serviceHeaderValid(request)) {
		sendJson(response, 401, {
			error: { message: 'Unauthorized service request' }
		});
		return;
	}

	readJsonBody(request)
		.then((body) => {
			if (!body || typeof body !== 'object') {
				sendJson(response, 400, {
					error: { message: 'Invalid JSON body' }
				});
				return;
			}

			const username = typeof body.username === 'string' ? body.username.trim() : '';
			const password = typeof body.password === 'string' ? body.password : '';

			if (!username || VALID_CREDENTIALS.get(username) !== password) {
				sendJson(response, 401, {
					error: { message: 'Invalid username or password' }
				});
				return;
			}

			const actorId = `actor_${username}`;
			const sessionId = `sess_${randomUUID()}`;
			const issuedAt = new Date().toISOString();
			const expiresAt = new Date(Date.now() + 60 * 60 * 1000).toISOString();

			sessions.set(sessionId, {
				actorId,
				provider: 'password_local'
			});

			sendJson(response, 200, {
				data: {
					session: {
						session_id: sessionId,
						actor_id: actorId,
						provider: 'password_local',
						issued_at: issuedAt,
						expires_at: expiresAt
					},
					actor: {
						actor_id: actorId,
						provider: 'password_local'
					}
				}
			});
		})
		.catch(() => {
			sendJson(response, 500, { error: { message: 'Mock server error' } });
		});
}

function handleRuns(request, response) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) {
		return;
	}

	sendJson(response, 200, {
		data: {
			items: RUNS
		}
	});
}

function handleGetRun(request, response, runId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	const run = RUN_DETAILS.get(runId);

	if (!run) {
		sendJson(response, 404, { error: { message: `Run ${runId} not found` } });
		return;
	}

	sendJson(response, 200, { data: run });
}

function handleSubmitRun(request, response) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	readJsonBody(request)
		.then((body) => {
			if (!body || typeof body !== 'object') {
				sendJson(response, 422, { error: { message: 'Invalid JSON body' } });
				return;
			}

			const target = body.target;
			if (
				typeof target !== 'object' ||
				target === null ||
				Array.isArray(target) ||
				(typeof target.type !== 'string' || typeof target.id !== 'string')
			) {
				sendJson(response, 422, {
					error: { message: 'Expected target with type and id' }
				});
				return;
			}

			withIdempotencyReplay(request, response, session, 'submit_run', () => ({
				status: 202,
				payload: {
					data: {
						run_id: 'run_submitted_001',
						status: 'queued',
						target: {
							type: target.type,
							id: target.id
						}
					}
				}
			}));
		})
		.catch(() => {
			sendJson(response, 500, { error: { message: 'Mock server error' } });
		});
}

function handleCancelRun(request, response, runId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	withIdempotencyReplay(request, response, session, `cancel_run:${runId}`, () => ({
		status: 200,
		payload: {
			data: {
				run_id: runId,
				status: 'cancelling'
			}
		}
	}));
}

function handleRerunRun(request, response, runId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	withIdempotencyReplay(request, response, session, `rerun_run:${runId}`, () => ({
		status: 202,
		payload: {
			data: {
				run_id: `${runId}_rerun_001`,
				status: 'queued'
			}
		}
	}));
}

function handleListManifests(request, response) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	sendJson(response, 200, { data: { items: MANIFESTS } });
}

function handleGetActiveManifest(request, response) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	sendJson(response, 200, {
		data: {
			manifest_version_id: 'manifest_v2',
			status: 'active'
		}
	});
}

function handleActivateManifest(request, response, manifestVersionId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	withIdempotencyReplay(request, response, session, `activate_manifest:${manifestVersionId}`, () => ({
		status: 200,
		payload: {
			data: {
				manifest_version_id: manifestVersionId,
				status: 'active'
			}
		}
	}));
}

function handleListSchedules(request, response) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	sendJson(response, 200, {
		data: {
			items: SCHEDULES
		}
	});
}

function handleScheduleDetail(request, response, scheduleId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	const schedule = SCHEDULE_DETAILS.get(scheduleId);

	if (!schedule) {
		sendJson(response, 404, { error: { message: `Schedule ${scheduleId} not found` } });
		return;
	}

	sendJson(response, 200, { data: schedule });
}

function handleRunStream(request, response, runId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	const lastEventId =
		typeof request.headers['last-event-id'] === 'string' ? request.headers['last-event-id'] : null;

	sendSse(response, 200, [
		'id: evt_001',
		'event: run_status',
		`data: ${JSON.stringify({
			run_id: runId,
			status: 'running',
			last_event_id_received: lastEventId
		})}`
	]);
}

const sockets = new Set();

const server = createServer((request, response) => {
	const method = request.method ?? 'GET';
	const url = new URL(request.url ?? '/', `http://${HOST}:${PORT}`);

	if (method === 'POST' && url.pathname === '/api/orchestrator/v1/auth/password/sessions') {
		handleLogin(request, response);
		return;
	}

	if (method === 'GET' && url.pathname === '/api/orchestrator/v1/runs') {
		handleRuns(request, response);
		return;
	}

	if (method === 'POST' && url.pathname === '/api/orchestrator/v1/runs') {
		handleSubmitRun(request, response);
		return;
	}

	if (method === 'GET' && url.pathname === '/api/orchestrator/v1/manifests') {
		handleListManifests(request, response);
		return;
	}

	if (method === 'GET' && url.pathname === '/api/orchestrator/v1/manifests/active') {
		handleGetActiveManifest(request, response);
		return;
	}

	if (method === 'GET' && url.pathname === '/api/orchestrator/v1/schedules') {
		handleListSchedules(request, response);
		return;
	}

	const runDetailMatch = url.pathname.match(/^\/api\/orchestrator\/v1\/runs\/([^/]+)$/);
	if (method === 'GET' && runDetailMatch) {
		handleGetRun(request, response, decodeURIComponent(runDetailMatch[1]));
		return;
	}

	const cancelRunMatch = url.pathname.match(/^\/api\/orchestrator\/v1\/runs\/([^/]+)\/cancel$/);
	if (method === 'POST' && cancelRunMatch) {
		handleCancelRun(request, response, decodeURIComponent(cancelRunMatch[1]));
		return;
	}

	const rerunRunMatch = url.pathname.match(/^\/api\/orchestrator\/v1\/runs\/([^/]+)\/rerun$/);
	if (method === 'POST' && rerunRunMatch) {
		handleRerunRun(request, response, decodeURIComponent(rerunRunMatch[1]));
		return;
	}

	const activateManifestMatch = url.pathname.match(
		/^\/api\/orchestrator\/v1\/manifests\/([^/]+)\/activate$/
	);
	if (method === 'POST' && activateManifestMatch) {
		handleActivateManifest(request, response, decodeURIComponent(activateManifestMatch[1]));
		return;
	}

	const scheduleDetailMatch = url.pathname.match(/^\/api\/orchestrator\/v1\/schedules\/([^/]+)$/);
	if (method === 'GET' && scheduleDetailMatch) {
		handleScheduleDetail(request, response, decodeURIComponent(scheduleDetailMatch[1]));
		return;
	}

	const runStreamMatch = url.pathname.match(/^\/api\/orchestrator\/v1\/streams\/runs\/([^/]+)$/);
	if (method === 'GET' && runStreamMatch) {
		handleRunStream(request, response, decodeURIComponent(runStreamMatch[1]));
		return;
	}

	sendJson(response, 404, {
		error: { message: `No mock route for ${method} ${url.pathname}` }
	});
});

server.on('connection', (socket) => {
	sockets.add(socket);
	socket.on('close', () => {
		sockets.delete(socket);
	});
});

server.listen(PORT, HOST, () => {
	console.log(`[mock-orchestrator] listening on http://${HOST}:${PORT}`);
});

function shutdown() {
	server.close(() => {
		process.exit(0);
	});

	if (typeof server.closeIdleConnections === 'function') {
		server.closeIdleConnections();
	}

	if (typeof server.closeAllConnections === 'function') {
		server.closeAllConnections();
	}

	setTimeout(() => {
		for (const socket of sockets) {
			socket.destroy();
		}
	}, 300).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
