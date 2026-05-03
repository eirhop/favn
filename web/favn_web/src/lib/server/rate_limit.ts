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

const MAX_RATE_LIMIT_BUCKETS = 10_000;
const PRUNE_INTERVAL_MS = 60_000;
const buckets = new Map<string, Bucket>();
let lastPrunedAt = 0;

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
	maybePruneRateLimitBuckets(now);

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
	enforceRateLimitBucketLimit();
	return { allowed: true };
}

export function peekRateLimit(
	key: string,
	policy: RateLimitPolicy,
	now = Date.now()
): RateLimitDecision {
	maybePruneRateLimitBuckets(now);

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
	lastPrunedAt = 0;
}

export function pruneRateLimitBuckets(now = Date.now()): void {
	for (const [key, bucket] of buckets) {
		if (bucket.resetAt <= now) buckets.delete(key);
	}
	lastPrunedAt = now;
}

export function rateLimitBucketCount(): number {
	return buckets.size;
}

function enforceRateLimitBucketLimit(): void {
	while (buckets.size > MAX_RATE_LIMIT_BUCKETS) {
		const oldest = buckets.keys().next().value as string | undefined;
		if (!oldest) return;
		buckets.delete(oldest);
	}
}

function maybePruneRateLimitBuckets(now: number): void {
	if (now >= lastPrunedAt + PRUNE_INTERVAL_MS) {
		pruneRateLimitBuckets(now);
	}
}
