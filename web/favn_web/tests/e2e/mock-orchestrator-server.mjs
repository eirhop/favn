import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';

const PORT = Number(process.env.FAVN_ORCHESTRATOR_PORT ?? 4101);
const HOST = '127.0.0.1';
const VALID_CREDENTIALS = new Map([
	['alice', 'password123'],
	['bob', 'password123']
]);

const RUNS = [
	{
		id: 'run_001',
		status: 'succeeded',
		target: { type: 'pipeline', id: 'DailySalesPipeline' },
		trigger: 'manual',
		started_at: '2026-04-27T14:32:10.000Z',
		duration_ms: 12400,
		assets_completed: 2,
		assets_total: 2,
		manifest_version_id: 'manifest_v1'
	},
	{
		id: 'run_002',
		status: 'failed',
		target: { type: 'pipeline', id: 'ImportCustomers' },
		trigger: 'manual',
		started_at: '2026-04-27T14:19:02.000Z',
		duration_ms: 3100,
		assets_completed: 3,
		assets_total: 5,
		manifest_version_id: 'manifest_v2'
	},
	{
		id: 'run_003',
		status: 'running',
		target: { type: 'pipeline', id: 'BuildWarehouse' },
		trigger: 'manual',
		started_at: '2026-04-27T14:36:51.000Z',
		duration_ms: 42000,
		assets_completed: 4,
		assets_total: 10,
		manifest_version_id: 'manifest_v2'
	}
];

const RUN_DETAILS = new Map([
	[
		'run_001',
		{
			id: 'run_001',
			status: 'succeeded',
			target: { type: 'pipeline', id: 'DailySalesPipeline' },
			manifest_version_id: 'manifest_v1',
			started_at: '2026-04-27T14:32:10.000Z',
			duration_ms: 12400,
			assets: [
				{
					id: 'Raw.Crm.Customers',
					asset: 'Raw.Crm.Customers',
					type: 'SQL',
					stage: 1,
					status: 'succeeded',
					started_at: '2026-04-27T14:32:10.000Z',
					duration_ms: 430,
					output: 'raw.crm_customers',
					outputs: [
						{
							relation: 'raw.crm_customers',
							type: 'table',
							connection: 'local_duckdb',
							rows: 10000,
							created_at: '2026-04-27T14:32:11.000Z'
						}
					]
				},
				{
					id: 'Raw.Crm.Orders',
					asset: 'Raw.Crm.Orders',
					type: 'SQL',
					stage: 1,
					status: 'succeeded',
					started_at: '2026-04-27T14:32:11.000Z',
					duration_ms: 611,
					output: 'raw.crm_orders',
					outputs: [
						{
							relation: 'raw.crm_orders',
							type: 'table',
							connection: 'local_duckdb',
							rows: 42991,
							created_at: '2026-04-27T14:32:12.000Z'
						}
					]
				}
			],
			outputs: [
				{
					relation: 'raw.crm_customers',
					type: 'table',
					asset: 'Raw.Crm.Customers',
					connection: 'local_duckdb',
					rows: 10000,
					created_at: '2026-04-27T14:32:11.000Z'
				},
				{
					relation: 'raw.crm_orders',
					type: 'table',
					asset: 'Raw.Crm.Orders',
					connection: 'local_duckdb',
					rows: 42991,
					created_at: '2026-04-27T14:32:12.000Z'
				}
			],
			events: [
				{
					id: 'evt_001',
					type: 'run_submitted',
					timestamp: '2026-04-27T14:32:10.000Z',
					message: 'DailySalesPipeline submitted by local-operator'
				},
				{
					id: 'evt_002',
					type: 'run_succeeded',
					timestamp: '2026-04-27T14:32:22.000Z',
					message: 'Run succeeded'
				}
			]
		}
	],
	[
		'run_002',
		{
			id: 'run_002',
			status: 'failed',
			target: { type: 'pipeline', id: 'ImportCustomers' },
			manifest_version_id: 'manifest_v2',
			started_at: '2026-04-27T14:19:02.000Z',
			duration_ms: 3100,
			error: 'DuckDB query failed: column "customer_id" not found',
			assets: [
				{
					id: 'Raw.Crm.Customers',
					asset: 'Raw.Crm.Customers',
					type: 'SQL',
					stage: 1,
					status: 'succeeded',
					started_at: '2026-04-27T14:19:02.000Z',
					duration_ms: 430,
					output: 'raw.crm_customers',
					connection: 'local_duckdb',
					outputs: [
						{
							relation: 'raw.crm_customers',
							type: 'table',
							connection: 'local_duckdb',
							rows: 10000,
							created_at: '2026-04-27T14:19:03.000Z'
						}
					]
				},
				{
					id: 'Raw.Crm.Orders',
					asset: 'Raw.Crm.Orders',
					type: 'SQL',
					stage: 1,
					status: 'succeeded',
					started_at: '2026-04-27T14:19:03.000Z',
					duration_ms: 611,
					output: 'raw.crm_orders',
					connection: 'local_duckdb',
					outputs: [
						{
							relation: 'raw.crm_orders',
							type: 'table',
							connection: 'local_duckdb',
							rows: 42991,
							created_at: '2026-04-27T14:19:04.000Z'
						}
					]
				},
				{
					id: 'Staging.CustomerOrders',
					asset: 'Staging.CustomerOrders',
					type: 'SQL',
					stage: 2,
					status: 'failed',
					started_at: '2026-04-27T14:19:04.000Z',
					duration_ms: 812,
					output: 'staging.customer_orders',
					connection: 'local_duckdb',
					database: '.favn/data/work.duckdb',
					operation: 'materialize table',
					sql: 'create or replace table staging.customer_orders as\nselect customer_id, order_date, total_amount\nfrom raw.crm_orders',
					error: 'column "customer_id" not found',
					outputs: [
						{
							relation: 'staging.customer_orders',
							type: 'table',
							connection: 'local_duckdb',
							status: 'failed'
						}
					]
				},
				{
					id: 'Staging.OrderSummary',
					asset: 'Staging.OrderSummary',
					type: 'SQL',
					stage: 2,
					status: 'cancelled',
					output: 'staging.order_summary',
					error: 'skipped'
				},
				{
					id: 'Mart.CustomerRevenue',
					asset: 'Mart.CustomerRevenue',
					type: 'SQL',
					stage: 3,
					status: 'cancelled',
					output: 'mart.customer_revenue',
					error: 'skipped'
				}
			],
			outputs: [
				{
					relation: 'raw.crm_customers',
					type: 'table',
					asset: 'Raw.Crm.Customers',
					connection: 'local_duckdb',
					rows: 10000,
					created_at: '2026-04-27T14:19:03.000Z'
				},
				{
					relation: 'raw.crm_orders',
					type: 'table',
					asset: 'Raw.Crm.Orders',
					connection: 'local_duckdb',
					rows: 42991,
					created_at: '2026-04-27T14:19:04.000Z'
				},
				{
					relation: 'staging.customer_orders',
					type: 'table',
					asset: 'Staging.CustomerOrders',
					connection: 'local_duckdb',
					status: 'failed'
				}
			],
			events: [
				{
					id: 'evt_101',
					type: 'run_submitted',
					timestamp: '2026-04-27T14:19:02.000Z',
					message: 'ImportCustomers submitted by local-operator'
				},
				{
					id: 'evt_102',
					type: 'manifest_selected',
					timestamp: '2026-04-27T14:19:02.000Z',
					message: 'manifest_v2'
				},
				{
					id: 'evt_103',
					type: 'asset_started',
					timestamp: '2026-04-27T14:19:04.000Z',
					asset_id: 'Staging.CustomerOrders',
					message: 'Staging.CustomerOrders'
				},
				{
					id: 'evt_104',
					type: 'asset_failed',
					timestamp: '2026-04-27T14:19:05.000Z',
					asset_id: 'Staging.CustomerOrders',
					message: 'Staging.CustomerOrders · column "customer_id" not found'
				},
				{
					id: 'evt_105',
					type: 'run_failed',
					timestamp: '2026-04-27T14:19:05.000Z',
					message: 'Run failed'
				}
			]
		}
	],
	[
		'run_003',
		{
			id: 'run_003',
			status: 'running',
			target: { type: 'pipeline', id: 'BuildWarehouse' },
			manifest_version_id: 'manifest_v2',
			started_at: '2026-04-27T14:36:51.000Z',
			duration_ms: 42000,
			assets: [
				{
					id: 'Raw.A',
					asset: 'Raw.A',
					type: 'SQL',
					stage: 1,
					status: 'succeeded',
					duration_ms: 150
				},
				{ id: 'Raw.B', asset: 'Raw.B', type: 'SQL', stage: 1, status: 'running' }
			]
		}
	]
]);

const MANIFESTS = [
	{ manifest_version_id: 'manifest_v1', status: 'inactive' },
	{ manifest_version_id: 'manifest_v2', status: 'active' }
];

const SCHEDULES = [
	{ schedule_id: 'sched_001', enabled: true, target: { type: 'asset', id: 'asset.orders' } },
	{
		schedule_id: 'sched_002',
		enabled: false,
		target: { type: 'pipeline', id: 'pipeline.reconcile' }
	}
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

function sendJson(response, status, payload) {
	response.writeHead(status, {
		'content-type': 'application/json; charset=utf-8'
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
				typeof target.type !== 'string' ||
				typeof target.id !== 'string'
			) {
				sendJson(response, 422, {
					error: { message: 'Expected target with type and id' }
				});
				return;
			}

			sendJson(response, 202, {
				data: {
					run_id: 'run_submitted_001',
					status: 'queued',
					target: {
						type: target.type,
						id: target.id
					}
				}
			});
		})
		.catch(() => {
			sendJson(response, 500, { error: { message: 'Mock server error' } });
		});
}

function handleCancelRun(request, response, runId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	sendJson(response, 200, {
		data: {
			run_id: runId,
			status: 'cancelling'
		}
	});
}

function handleRerunRun(request, response, runId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	sendJson(response, 202, {
		data: {
			run_id: `${runId}_rerun_001`,
			status: 'queued'
		}
	});
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
			manifest: {
				manifest_version_id: 'manifest_v2',
				status: 'active'
			},
			targets: [
				{ type: 'asset', id: 'asset.orders' },
				{ type: 'pipeline', id: 'pipeline.reconcile' }
			]
		}
	});
}

function handleActivateManifest(request, response, manifestVersionId) {
	const session = requireAuthenticatedSession(request, response);
	if (!session) return;

	sendJson(response, 200, {
		data: {
			manifest_version_id: manifestVersionId,
			status: 'active'
		}
	});
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
