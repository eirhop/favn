import { describe, expect, it } from 'vitest';
import {
	FAVN_WEB_SESSION_COOKIE,
	readWebSessionCookie,
	setWebSessionCookie,
	webSessionFromLoginPayload,
	type WebSession
} from './session';

type CookieSetOptions = Record<string, unknown>;

class MockCookies {
	private readonly values = new Map<string, string>();
	public setCalls: Array<{ name: string; value: string; options: CookieSetOptions }> = [];

	get(name: string): string | undefined {
		return this.values.get(name);
	}

	set(name: string, value: string, options: CookieSetOptions): void {
		this.values.set(name, value);
		this.setCalls.push({ name, value, options });
	}

	delete(name: string): void {
		this.values.delete(name);
	}
}

function createSession(overrides: Partial<WebSession> = {}): WebSession {
	return {
		session_id: 'sess-1',
		actor_id: 'actor-1',
		provider: 'password_local',
		expires_at: '2999-01-01T00:00:00.000Z',
		issued_at: '2026-01-01T00:00:00.000Z',
		...overrides
	};
}

describe('readWebSessionCookie', () => {
	it('returns null for invalid cookie encoding', () => {
		const cookies = new MockCookies();
		cookies.set(FAVN_WEB_SESSION_COOKIE, 'not-base64url-json', {});

		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});

	it('returns null for malformed decoded payload', () => {
		const cookies = new MockCookies();
		const malformed = Buffer.from(JSON.stringify({ session_id: 'sess-1' }), 'utf8').toString(
			'base64url'
		);
		cookies.set(FAVN_WEB_SESSION_COOKIE, malformed, {});

		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});

	it('returns null when cookie session is expired', () => {
		const cookies = new MockCookies();
		setWebSessionCookie(
			cookies as never,
			createSession({ expires_at: '2000-01-01T00:00:00.000Z' })
		);

		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});

	it('returns decoded session for valid non-expired cookie', () => {
		const cookies = new MockCookies();
		const session = createSession();
		setWebSessionCookie(cookies as never, session);

		expect(readWebSessionCookie(cookies as never)).toEqual(session);
	});

	it('returns null for tampered signed payload', () => {
		const cookies = new MockCookies();
		setWebSessionCookie(cookies as never, createSession());

		const encoded = cookies.get(FAVN_WEB_SESSION_COOKIE);
		expect(encoded).toBeTruthy();

		if (!encoded) {
			throw new Error('Expected encoded cookie to be present');
		}

		const [payload, signature] = encoded.split('.');
		const tamperedPayload = `${payload}x`;
		cookies.set(FAVN_WEB_SESSION_COOKIE, `${tamperedPayload}.${signature}`, {});

		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});
});

describe('webSessionFromLoginPayload', () => {
	it('normalizes nested data/session/actor payloads', () => {
		const session = webSessionFromLoginPayload({
			data: {
				session: {
					session_id: 'sess-2',
					provider: 'password_local',
					expires_at: '2999-01-01T00:00:00.000Z'
				},
				actor: {
					id: 'actor-2'
				}
			}
		});

		expect(session).toEqual({
			session_id: 'sess-2',
			actor_id: 'actor-2',
			provider: 'password_local',
			expires_at: '2999-01-01T00:00:00.000Z',
			issued_at: null
		});
	});

	it('falls back to session.id + actor.id and default provider', () => {
		const session = webSessionFromLoginPayload({
			data: {
				session: {
					id: 'sess-3',
					provider: '',
					created_at: '2026-01-02T00:00:00.000Z'
				},
				actor: {
					id: 'actor-3'
				}
			}
		});

		expect(session).toEqual({
			session_id: 'sess-3',
			actor_id: 'actor-3',
			provider: 'password_local',
			expires_at: null,
			issued_at: '2026-01-02T00:00:00.000Z'
		});
	});

	it('returns null when required identifiers are missing', () => {
		expect(
			webSessionFromLoginPayload({ data: { session: { provider: 'password_local' } } })
		).toBeNull();
		expect(webSessionFromLoginPayload(null)).toBeNull();
	});
});
