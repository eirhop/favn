import { error, redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoad } from './$types';
import { clearWebSessionCookie } from '$lib/server/session';
import { orchestratorListRuns } from '$lib/server/orchestrator';

type RunSummary = {
	id: string;
	status: string | null;
};

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
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
			return { id: `run-${index + 1}`, status: null };
		}

		const id = asString(run.id) ?? asString(run.run_id) ?? `run-${index + 1}`;
		const status = asString(run.status);

		return { id, status };
	});
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

	const response = await orchestratorListRuns(locals.session);

	if (response.status === 401) {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}

	if (!response.ok) {
		throw error(response.status, 'Failed to load runs');
	}

	const payload = await readJsonOr(response, []);

	return {
		session: locals.session,
		runs: normalizeRuns(payload)
	};
};

export const actions: Actions = {
	logout: async ({ cookies, locals }) => {
		clearWebSessionCookie(cookies);
		locals.session = null;
		throw redirect(303, '/login');
	}
};
