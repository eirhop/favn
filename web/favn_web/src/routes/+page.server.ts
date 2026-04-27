import { error, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie } from '$lib/server/session';
import {
	orchestratorGetActiveManifest,
	orchestratorListRuns,
	orchestratorListSchedules
} from '$lib/server/orchestrator';

type RunSummary = {
	id: string;
	status: string | null;
	target: string | null;
};

type ScheduleSummary = {
	id: string;
	enabled: boolean | null;
	target: string | null;
};

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
}

function targetLabel(value: unknown): string | null {
	if (!isRecord(value)) return null;
	const type = asString(value.type);
	const id = asString(value.id);
	return type && id ? `${type}:${id}` : id;
}

function normalizeRuns(payload: unknown): RunSummary[] {
	const dataObj = isRecord(payload) && isRecord(payload.data) ? payload.data : payload;

	const list = Array.isArray(dataObj)
		? dataObj
		: isRecord(dataObj) && Array.isArray(dataObj.items)
			? dataObj.items
			: [];

	return list.map((run, index) => {
		if (!isRecord(run)) {
			return { id: `run-${index + 1}`, status: null, target: null };
		}

		const id = asString(run.id) ?? asString(run.run_id) ?? `run-${index + 1}`;
		const status = asString(run.status);
		const target = targetLabel(run.target);

		return { id, status, target };
	});
}

function normalizeSchedules(payload: unknown): ScheduleSummary[] {
	const dataObj = isRecord(payload) && isRecord(payload.data) ? payload.data : payload;
	const list = isRecord(dataObj) && Array.isArray(dataObj.items) ? dataObj.items : [];

	return list.map((schedule, index) => {
		if (!isRecord(schedule)) {
			return { id: `schedule-${index + 1}`, enabled: null, target: null };
		}

		return {
			id: asString(schedule.schedule_id) ?? asString(schedule.id) ?? `schedule-${index + 1}`,
			enabled: typeof schedule.enabled === 'boolean' ? schedule.enabled : null,
			target: targetLabel(schedule.target)
		};
	});
}

function normalizeActiveManifest(payload: unknown): string | null {
	const dataObj = isRecord(payload) && isRecord(payload.data) ? payload.data : payload;
	return isRecord(dataObj) ? (asString(dataObj.manifest_version_id) ?? asString(dataObj.id)) : null;
}

async function readJsonOr(response: Response, fallback: unknown): Promise<unknown> {
	try {
		return await response.json();
	} catch {
		return fallback;
	}
}

export const load: PageServerLoad = async ({ locals, cookies }) => {
	if (!locals.session) {
		throw redirect(303, '/login');
	}

	const [runsResponse, activeManifestResponse, schedulesResponse] = await Promise.all([
		orchestratorListRuns(locals.session),
		orchestratorGetActiveManifest(locals.session),
		orchestratorListSchedules(locals.session)
	]);

	if (runsResponse.status === 401 && locals.session.provider !== 'web_local_admin') {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}

	if (!runsResponse.ok && runsResponse.status !== 401) {
		throw error(runsResponse.status, 'Failed to load runs');
	}

	const runsPayload = runsResponse.ok ? await readJsonOr(runsResponse, []) : [];
	const activeManifestPayload = activeManifestResponse.ok
		? await readJsonOr(activeManifestResponse, null)
		: null;
	const schedulesPayload = schedulesResponse.ok ? await readJsonOr(schedulesResponse, []) : [];

	return {
		session: locals.session,
		runs: normalizeRuns(runsPayload),
		activeManifestVersionId: normalizeActiveManifest(activeManifestPayload),
		schedules: normalizeSchedules(schedulesPayload),
		orchestratorWarning:
			runsResponse.status === 401 && locals.session.provider === 'web_local_admin'
				? 'Signed in with web-local admin credentials. Configure matching orchestrator credentials for live control-plane data.'
				: null
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}
};
