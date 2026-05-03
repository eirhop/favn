import { afterEach, describe, expect, it } from 'vitest';
import { checkLoginAllowed, clearLoginThrottleFor, recordFailedLogin } from './login_throttle';
import { resetAllRateLimits } from './rate_limit';

const context = { getClientAddress: () => '203.0.113.10' };

afterEach(() => resetAllRateLimits());

describe('login throttle', () => {
	it('limits repeated failures for normalized username and client address', () => {
		for (let attempt = 0; attempt < 5; attempt += 1) {
			expect(checkLoginAllowed(context, 'Alice')).toEqual({ allowed: true });
			recordFailedLogin(context, 'alice');
		}

		expect(checkLoginAllowed(context, ' ALICE ')).toMatchObject({
			allowed: false,
			retryAfterSeconds: expect.any(Number)
		});
	});

	it('clears username/client throttle after successful login', () => {
		for (let attempt = 0; attempt < 5; attempt += 1) {
			recordFailedLogin(context, 'alice');
		}

		clearLoginThrottleFor(context, 'alice');

		expect(checkLoginAllowed(context, 'alice')).toEqual({ allowed: true });
	});
});
