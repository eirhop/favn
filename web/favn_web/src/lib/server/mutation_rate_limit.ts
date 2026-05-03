import type { RequestEvent } from '@sveltejs/kit';
import { checkRateLimit, clientAddress, type RateLimitDecision } from './rate_limit';

const MUTATION_POLICY = { limit: 120, windowMs: 60 * 1000 };

function mutationKey(event: Pick<RequestEvent, 'getClientAddress' | 'locals'>): string {
	const session = event.locals.session;
	if (session) {
		return `mutation:session:${session.actor_id}:${session.session_id || 'unknown-session'}`;
	}

	return `mutation:client:${clientAddress(event)}`;
}

export function checkMutationRateLimit(
	event: Pick<RequestEvent, 'getClientAddress' | 'locals'>,
	now = Date.now()
): RateLimitDecision {
	return checkRateLimit(mutationKey(event), MUTATION_POLICY, now);
}
