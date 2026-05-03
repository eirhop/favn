import type { RequestEvent } from '@sveltejs/kit';
import {
	checkRateLimit,
	clientAddress,
	peekRateLimit,
	resetRateLimit,
	type RateLimitDecision
} from './rate_limit';

const USERNAME_CLIENT_POLICY = { limit: 5, windowMs: 10 * 60 * 1000 };
const CLIENT_ONLY_POLICY = { limit: 20, windowMs: 10 * 60 * 1000 };

function normalizeUsername(username: string): string {
	return username.trim().toLocaleLowerCase();
}

function usernameClientKey(username: string, address: string): string {
	return `login:username-client:${normalizeUsername(username)}:${address}`;
}

function clientOnlyKey(address: string): string {
	return `login:client:${address}`;
}

export function checkLoginAllowed(
	event: Pick<RequestEvent, 'getClientAddress'>,
	username: string,
	now = Date.now()
): RateLimitDecision {
	const address = clientAddress(event);
	const clientDecision = peekRateLimit(clientOnlyKey(address), CLIENT_ONLY_POLICY, now);
	if (!clientDecision.allowed) return clientDecision;

	return peekRateLimit(usernameClientKey(username, address), USERNAME_CLIENT_POLICY, now);
}

export function recordFailedLogin(
	event: Pick<RequestEvent, 'getClientAddress'>,
	username: string,
	now = Date.now()
): void {
	const address = clientAddress(event);
	checkRateLimit(clientOnlyKey(address), CLIENT_ONLY_POLICY, now);
	checkRateLimit(usernameClientKey(username, address), USERNAME_CLIENT_POLICY, now);
}

export function clearLoginThrottleFor(
	event: Pick<RequestEvent, 'getClientAddress'>,
	username: string
): void {
	const address = clientAddress(event);
	resetRateLimit(usernameClientKey(username, address));
}
