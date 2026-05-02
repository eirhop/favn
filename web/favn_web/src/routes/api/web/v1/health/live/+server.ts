import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = () => {
	return json(
		{ service: 'favn_web', status: 'ok' },
		{
			status: 200,
			headers: { 'cache-control': 'no-store' }
		}
	);
};
