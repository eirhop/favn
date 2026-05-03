import { afterEach, describe, expect, it } from 'vitest';
import { checkRateLimit, peekRateLimit, resetAllRateLimits } from './rate_limit';

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
});
