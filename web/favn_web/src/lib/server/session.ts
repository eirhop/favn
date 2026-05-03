import type { Cookies } from '@sveltejs/kit';
import { randomBytes } from 'node:crypto';

export const FAVN_WEB_SESSION_COOKIE = '__Host-favn_web_session';

const WEB_SESSION_ID_BYTES = 32;
const WEB_SESSION_ID_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const MAX_WEB_SESSION_STORE_SIZE = 10_000;

export type WebSession = {
	session_token: string;
	session_id: string;
	actor_id: string;
	provider: string;
	expires_at: string | null;
	issued_at: string | null;
};

export type PublicWebSession = Omit<WebSession, 'session_token'>;

type JsonRecord = Record<string, unknown>;

type StoredWebSession = {
	session: WebSession;
	expiresAt: number | null;
};

const webSessionStore = new Map<string, StoredWebSession>();

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
}

function parseDate(value: string | null): Date | null {
	if (!value) return null;
	const date = new Date(value);
	return Number.isNaN(date.getTime()) ? null : date;
}

function generateWebSessionId(): string {
	return randomBytes(WEB_SESSION_ID_BYTES).toString('base64url');
}

function validWebSessionId(value: string): boolean {
	return WEB_SESSION_ID_PATTERN.test(value);
}

function pruneExpiredWebSessions(now = Date.now()): void {
	for (const [webSessionId, stored] of webSessionStore) {
		if (stored.expiresAt !== null && stored.expiresAt <= now) {
			webSessionStore.delete(webSessionId);
		}
	}
}

function enforceWebSessionStoreLimit(): void {
	while (webSessionStore.size > MAX_WEB_SESSION_STORE_SIZE) {
		const oldest = webSessionStore.keys().next().value as string | undefined;
		if (!oldest) return;
		webSessionStore.delete(oldest);
	}
}

function storeWebSession(webSessionId: string, session: WebSession, now = Date.now()): void {
	pruneExpiredWebSessions(now);

	const expires = parseDate(session.expires_at);
	webSessionStore.set(webSessionId, {
		session,
		expiresAt: expires ? expires.getTime() : null
	});
	enforceWebSessionStoreLimit();
}

function readStoredWebSession(webSessionId: string, now = Date.now()): WebSession | null {
	const stored = webSessionStore.get(webSessionId);
	if (!stored) return null;

	if (stored.expiresAt !== null && stored.expiresAt <= now) {
		webSessionStore.delete(webSessionId);
		return null;
	}

	return stored.session;
}

export function readWebSessionCookie(cookies: Cookies): WebSession | null {
	const webSessionId = cookies.get(FAVN_WEB_SESSION_COOKIE);
	if (!webSessionId || !validWebSessionId(webSessionId)) return null;

	return readStoredWebSession(webSessionId);
}

export function setWebSessionCookie(cookies: Cookies, session: WebSession): string {
	const expires = parseDate(session.expires_at);
	const maxAge = expires ? Math.max(0, Math.floor((expires.getTime() - Date.now()) / 1000)) : null;
	const webSessionId = generateWebSessionId();

	storeWebSession(webSessionId, session);

	cookies.set(FAVN_WEB_SESSION_COOKIE, webSessionId, {
		httpOnly: true,
		sameSite: 'strict',
		secure: true,
		path: '/',
		...(expires ? { expires } : {}),
		...(maxAge !== null ? { maxAge } : {})
	});

	return webSessionId;
}

export function clearWebSessionCookie(cookies: Cookies): void {
	const webSessionId = cookies.get(FAVN_WEB_SESSION_COOKIE);
	if (webSessionId && validWebSessionId(webSessionId)) {
		webSessionStore.delete(webSessionId);
	}

	cookies.delete(FAVN_WEB_SESSION_COOKIE, {
		path: '/'
	});
}

export function resetWebSessionStore(): void {
	webSessionStore.clear();
}

export function webSessionStoreSize(): number {
	return webSessionStore.size;
}

export function pruneWebSessionStore(now = Date.now()): void {
	pruneExpiredWebSessions(now);
}

export function publicWebSession(session: WebSession): PublicWebSession {
	return {
		session_id: session.session_id,
		actor_id: session.actor_id,
		provider: session.provider,
		expires_at: session.expires_at,
		issued_at: session.issued_at
	};
}

export function webSessionFromLoginPayload(payload: unknown): WebSession | null {
	if (!isRecord(payload)) return null;

	const dataObj = isRecord(payload.data) ? payload.data : payload;
	const sessionObj = isRecord(dataObj.session) ? dataObj.session : dataObj;
	const actorObj = isRecord(dataObj.actor) ? dataObj.actor : null;

	const session_token = asString(dataObj.session_token) ?? asString(sessionObj.session_token);
	const session_id =
		asString(sessionObj.session_id) ?? asString(sessionObj.id) ?? asString(dataObj.session_id);
	const actor_id =
		asString(sessionObj.actor_id) ??
		asString(actorObj?.actor_id) ??
		asString(actorObj?.id) ??
		asString(dataObj.actor_id);

	if (!session_token || !actor_id) return null;

	return {
		session_token,
		session_id: session_id ?? '',
		actor_id,
		provider: asString(sessionObj.provider) ?? asString(dataObj.provider) ?? 'password_local',
		expires_at: asString(sessionObj.expires_at) ?? asString(dataObj.expires_at),
		issued_at:
			asString(sessionObj.issued_at) ??
			asString(sessionObj.created_at) ??
			asString(dataObj.issued_at) ??
			asString(dataObj.created_at)
	};
}
