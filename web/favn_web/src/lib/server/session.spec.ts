import { afterEach, describe, expect, it } from 'vitest';
import {
	clearWebSessionCookie,
	FAVN_WEB_SESSION_COOKIE,
	pruneWebSessionStore,
	publicWebSession,
	readWebSessionCookie,
	resetWebSessionStore,
	setWebSessionCookie,
	webSessionStoreSize,
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

afterEach(() => resetWebSessionStore());

function createSession(overrides: Partial<WebSession> = {}): WebSession {
	return {
		session_token: 'opaque-session-token-1',
		session_id: 'sess-1',
		actor_id: 'actor-1',
		provider: 'password_local',
		expires_at: '2999-01-01T00:00:00.000Z',
		issued_at: '2026-01-01T00:00:00.000Z',
		...overrides
	};
}

describe('readWebSessionCookie', () => {
	it('returns null for invalid web session id encoding', () => {
		const cookies = new MockCookies();
		cookies.set(FAVN_WEB_SESSION_COOKIE, 'not-a-valid-web-session-id', {});

		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});

	it('returns null for syntactically valid but unknown web session id', () => {
		const cookies = new MockCookies();
		cookies.set(FAVN_WEB_SESSION_COOKIE, 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', {});

		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});

	it('returns null when cookie session is expired', () => {
		const cookies = new MockCookies();
		setWebSessionCookie(
			cookies as never,
			createSession({ expires_at: '2000-01-01T00:00:00.000Z' })
		);

		expect(readWebSessionCookie(cookies as never)).toBeNull();
		expect(webSessionStoreSize()).toBe(0);
	});

	it('returns server-side session for valid non-expired web session cookie', () => {
		const cookies = new MockCookies();
		const session = createSession();
		setWebSessionCookie(cookies as never, session);

		expect(readWebSessionCookie(cookies as never)).toEqual(session);
	});

	it('sets only an opaque web session id in the browser cookie', () => {
		const cookies = new MockCookies();
		const session = createSession({ session_token: 'raw-orchestrator-session-token' });
		const webSessionId = setWebSessionCookie(cookies as never, session);
		const cookieValue = cookies.get(FAVN_WEB_SESSION_COOKIE);

		expect(cookieValue).toBe(webSessionId);
		expect(cookieValue).toMatch(/^[A-Za-z0-9_-]{43}$/);
		expect(cookieValue).not.toContain(session.session_token);
		expect(cookieValue).not.toContain('actor-1');
	});

	it('sets safe bounded browser session cookie options', () => {
		const cookies = new MockCookies();
		const expiresAt = new Date(Date.now() + 60 * 60 * 1000).toISOString();
		setWebSessionCookie(cookies as never, createSession({ expires_at: expiresAt }));

		expect(cookies.setCalls).toHaveLength(1);
		expect(cookies.setCalls[0].name).toBe(FAVN_WEB_SESSION_COOKIE);
		expect(cookies.setCalls[0].options).toMatchObject({
			httpOnly: true,
			sameSite: 'strict',
			path: '/',
			maxAge: expect.any(Number)
		});
		expect(cookies.setCalls[0].options).not.toHaveProperty('domain');
	});

	it('deletes the server-side session when clearing the cookie', () => {
		const cookies = new MockCookies();
		setWebSessionCookie(cookies as never, createSession());

		expect(webSessionStoreSize()).toBe(1);
		clearWebSessionCookie(cookies as never);
		expect(webSessionStoreSize()).toBe(0);
		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});

	it('prunes expired server-side sessions', () => {
		const cookies = new MockCookies();
		setWebSessionCookie(
			cookies as never,
			createSession({ expires_at: '2026-01-01T00:00:00.000Z' })
		);

		expect(webSessionStoreSize()).toBe(1);
		pruneWebSessionStore(new Date('2026-01-01T00:00:00.000Z').getTime());
		expect(webSessionStoreSize()).toBe(0);
	});

	it('returns null for tampered web session id', () => {
		const cookies = new MockCookies();
		setWebSessionCookie(cookies as never, createSession());

		cookies.set(FAVN_WEB_SESSION_COOKIE, 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', {});

		expect(readWebSessionCookie(cookies as never)).toBeNull();
	});
});

describe('webSessionFromLoginPayload', () => {
	it('normalizes nested data/session/actor payloads', () => {
		const session = webSessionFromLoginPayload({
			data: {
				session_token: 'opaque-token-2',
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
			session_token: 'opaque-token-2',
			session_id: 'sess-2',
			actor_id: 'actor-2',
			provider: 'password_local',
			expires_at: '2999-01-01T00:00:00.000Z',
			issued_at: null
		});
	});

	it('keeps safe session metadata without treating session id as the bearer token', () => {
		const session = webSessionFromLoginPayload({
			data: {
				session: {
					session_token: 'opaque-token-3',
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
			session_token: 'opaque-token-3',
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
		expect(
			webSessionFromLoginPayload({ data: { session_id: 'sess-4', actor_id: 'actor-4' } })
		).toBeNull();
		expect(webSessionFromLoginPayload(null)).toBeNull();
	});
});

describe('publicWebSession', () => {
	it('omits the raw opaque session token from browser-facing session data', () => {
		expect(publicWebSession(createSession())).toEqual({
			session_id: 'sess-1',
			actor_id: 'actor-1',
			provider: 'password_local',
			expires_at: '2999-01-01T00:00:00.000Z',
			issued_at: '2026-01-01T00:00:00.000Z'
		});
	});
});
