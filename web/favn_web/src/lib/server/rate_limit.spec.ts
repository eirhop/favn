import { afterEach, describe, expect, it } from 'vitest';
import {
	checkRateLimit,
	peekRateLimit,
	pruneRateLimitBuckets,
	rateLimitBucketCount,
	resetAllRateLimits
} from './rate_limit';

afterEach(() => resetAllRateLimits());

describe('rate limiter', () => {
	it('limits requests within a bounded window', () => {
		expect(checkRateLimit('key', { limit: 2, windowMs: 1000 }, 0)).toEqual({ allowed: true });
		expect(checkRateLimit('key', { limit: 2, windowMs: 1000 }, 1)).toEqual({ allowed: true });
		expect(checkRateLimit('key', { limit: 2, windowMs: 1000 }, 2)).toMatchObject({
			allowed: false,
			retryAfterSeconds: 1
		});
	});

	it('peeks without consuming a request', () => {
		expect(peekRateLimit('key', { limit: 1, windowMs: 1000 }, 0)).toEqual({ allowed: true });
		expect(checkRateLimit('key', { limit: 1, windowMs: 1000 }, 0)).toEqual({ allowed: true });
		expect(peekRateLimit('key', { limit: 1, windowMs: 1000 }, 0)).toMatchObject({
			allowed: false
		});
	});

	it('resets after the window expires', () => {
		expect(checkRateLimit('key', { limit: 1, windowMs: 1000 }, 0)).toEqual({ allowed: true });
		expect(checkRateLimit('key', { limit: 1, windowMs: 1000 }, 1001)).toEqual({
			allowed: true
		});
	});

	it('prunes expired buckets across keys', () => {
		expect(checkRateLimit('key-1', { limit: 1, windowMs: 1000 }, 0)).toEqual({ allowed: true });
		expect(checkRateLimit('key-2', { limit: 1, windowMs: 2000 }, 0)).toEqual({ allowed: true });

		pruneRateLimitBuckets(1001);

		expect(rateLimitBucketCount()).toBe(1);
		expect(peekRateLimit('key-2', { limit: 1, windowMs: 2000 }, 1001)).toMatchObject({
			allowed: false
		});
	});

	it('evicts oldest buckets after reaching the process-local bound', () => {
		for (let index = 0; index < 10_001; index += 1) {
			checkRateLimit(`key-${index}`, { limit: 1, windowMs: 60_000 }, 0);
		}

		expect(rateLimitBucketCount()).toBe(10_000);
		expect(peekRateLimit('key-0', { limit: 1, windowMs: 60_000 }, 0)).toEqual({ allowed: true });
		expect(peekRateLimit('key-1', { limit: 1, windowMs: 60_000 }, 0)).toMatchObject({
			allowed: false
		});
	});
});
