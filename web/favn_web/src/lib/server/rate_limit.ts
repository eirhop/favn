import type { RequestEvent } from '@sveltejs/kit';

export type RateLimitDecision =
	| { allowed: true }
	| { allowed: false; retryAfterSeconds: number; resetAt: number };

export type RateLimitPolicy = {
	limit: number;
	windowMs: number;
};

type Bucket = {
	count: number;
	resetAt: number;
};

const buckets = new Map<string, Bucket>();

export function clientAddress(event: Pick<RequestEvent, 'getClientAddress'>): string {
	try {
		return event.getClientAddress() || 'unknown';
	} catch {
		return 'unknown';
	}
}

export function checkRateLimit(
	key: string,
	policy: RateLimitPolicy,
	now = Date.now()
): RateLimitDecision {
	const existing = buckets.get(key);
	const bucket =
		existing && existing.resetAt > now ? existing : { count: 0, resetAt: now + policy.windowMs };

	if (bucket.count >= policy.limit) {
		buckets.set(key, bucket);
		return {
			allowed: false,
			retryAfterSeconds: Math.max(1, Math.ceil((bucket.resetAt - now) / 1000)),
			resetAt: bucket.resetAt
		};
	}

	bucket.count += 1;
	buckets.set(key, bucket);
	return { allowed: true };
}

export function peekRateLimit(
	key: string,
	policy: RateLimitPolicy,
	now = Date.now()
): RateLimitDecision {
	const existing = buckets.get(key);
	if (!existing || existing.resetAt <= now || existing.count < policy.limit)
		return { allowed: true };

	return {
		allowed: false,
		retryAfterSeconds: Math.max(1, Math.ceil((existing.resetAt - now) / 1000)),
		resetAt: existing.resetAt
	};
}

export function resetRateLimit(key: string): void {
	buckets.delete(key);
}

export function resetAllRateLimits(): void {
	buckets.clear();
}
