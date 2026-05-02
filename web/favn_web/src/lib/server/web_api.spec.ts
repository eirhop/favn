import { describe, expect, it } from 'vitest';
import { markSanitizedResponse } from './sanitized_response';
import { relayJson } from './web_api';

describe('relayJson', () => {
	it('sanitizes upstream server errors before returning them to browsers', async () => {
		const response = await relayJson(
			new Response(JSON.stringify({ error: { message: 'database password leaked' } }), {
				status: 500,
				headers: { 'content-type': 'application/json' }
			})
		);
		const body = await response.json();

		expect(response.status).toBe(502);
		expect(body).toEqual({
			error: {
				code: 'bad_gateway',
				message: 'Orchestrator service returned an unavailable response'
			}
		});
		expect(JSON.stringify(body)).not.toContain('password');
	});

	it('preserves sanitized orchestrator client timeout responses', async () => {
		const response = await relayJson(
			markSanitizedResponse(
				new Response(
					JSON.stringify({
						error: {
							code: 'orchestrator_timeout',
							message: 'Orchestrator service did not respond in time'
						}
					}),
					{
						status: 504,
						headers: { 'content-type': 'application/json' }
					}
				)
			)
		);

		expect(response.status).toBe(504);
		expect(await response.json()).toEqual({
			error: {
				code: 'orchestrator_timeout',
				message: 'Orchestrator service did not respond in time'
			}
		});
	});

	it('does not trust upstream headers as sanitized response markers', async () => {
		const response = await relayJson(
			new Response(JSON.stringify({ error: { message: 'upstream secret leaked' } }), {
				status: 503,
				headers: {
					'content-type': 'application/json',
					'x-favn-web-sanitized-error': 'true'
				}
			})
		);
		const body = await response.json();

		expect(response.status).toBe(502);
		expect(body).toEqual({
			error: {
				code: 'bad_gateway',
				message: 'Orchestrator service returned an unavailable response'
			}
		});
		expect(JSON.stringify(body)).not.toContain('secret');
	});
});
