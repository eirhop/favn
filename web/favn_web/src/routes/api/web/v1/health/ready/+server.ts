import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { checkWebReadiness } from '$lib/server/readiness';

export const GET: RequestHandler = async () => {
	const report = await checkWebReadiness();

	return json(report, {
		status: report.status === 'ready' ? 200 : 503,
		headers: { 'cache-control': 'no-store' }
	});
};
