import { dev } from '$app/environment';
import { env } from '$env/dynamic/private';
import type { Cookies } from '@sveltejs/kit';
import { createHmac, timingSafeEqual } from 'node:crypto';

export const FAVN_WEB_SESSION_COOKIE = 'favn_web_session';

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

function isRecord(value: unknown): value is JsonRecord {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string | null {
	return typeof value === 'string' && value.length > 0 ? value : null;
}

function encodeSession(session: WebSession): string {
	const payload = Buffer.from(JSON.stringify(session), 'utf8').toString('base64url');
	const signature = sessionSignature(payload);
	return `${payload}.${signature}`;
}

function decodeSession(value: string): WebSession | null {
	try {
		const parts = value.split('.');
		if (parts.length !== 2) return null;

		const [payload, signature] = parts;
		if (!payload || !signature) return null;

		if (!sessionSignatureMatches(payload, signature)) return null;

		const parsed = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')) as unknown;
		if (!isRecord(parsed)) return null;

		const session_token = asString(parsed.session_token);
		const session_id = asString(parsed.session_id);
		const actor_id = asString(parsed.actor_id);
		const provider = asString(parsed.provider);

		if (!session_token || !actor_id || !provider) return null;

		return {
			session_token,
			session_id: session_id ?? '',
			actor_id,
			provider,
			expires_at: asString(parsed.expires_at),
			issued_at: asString(parsed.issued_at)
		};
	} catch {
		return null;
	}
}

function sessionSecret(): string {
	const configured = env.FAVN_WEB_SESSION_SECRET;

	if (configured && configured.length > 0) {
		return configured;
	}

	if (dev || env.NODE_ENV === 'test') {
		return 'favn-web-dev-session-secret-change-me';
	}

	throw new Error('Missing FAVN_WEB_SESSION_SECRET for session cookie signing');
}

function sessionSignature(payload: string): string {
	return createHmac('sha256', sessionSecret()).update(payload).digest('base64url');
}

function sessionSignatureMatches(payload: string, provided: string): boolean {
	const expected = sessionSignature(payload);
	const expectedBuffer = Buffer.from(expected, 'utf8');
	const providedBuffer = Buffer.from(provided, 'utf8');

	if (expectedBuffer.length !== providedBuffer.length) return false;

	return timingSafeEqual(expectedBuffer, providedBuffer);
}

function parseDate(value: string | null): Date | null {
	if (!value) return null;
	const date = new Date(value);
	return Number.isNaN(date.getTime()) ? null : date;
}

export function readWebSessionCookie(cookies: Cookies): WebSession | null {
	const raw = cookies.get(FAVN_WEB_SESSION_COOKIE);
	if (!raw) return null;

	const session = decodeSession(raw);
	if (!session) return null;

	const expiresAt = parseDate(session.expires_at);
	if (expiresAt && expiresAt.getTime() <= Date.now()) return null;

	return session;
}

export function setWebSessionCookie(cookies: Cookies, session: WebSession): void {
	const expires = parseDate(session.expires_at);

	cookies.set(FAVN_WEB_SESSION_COOKIE, encodeSession(session), {
		httpOnly: true,
		sameSite: 'lax',
		secure: !dev,
		path: '/',
		...(expires ? { expires } : {})
	});
}

export function clearWebSessionCookie(cookies: Cookies): void {
	cookies.delete(FAVN_WEB_SESSION_COOKIE, {
		path: '/'
	});
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
