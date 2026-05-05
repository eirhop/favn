import type { RequestEvent } from '@sveltejs/kit';
import { isIP } from 'node:net';
import { currentWebRuntimeEnv } from './runtime_config';
import type { WebSession } from './session';

export const LOCAL_DEV_TRUSTED_AUTH_HEADER = 'x-favn-local-dev-context';
export const LOCAL_DEV_TRUSTED_AUTH_HEADER_VALUE = 'trusted';
export const LOCAL_DEV_TRUSTED_PROVIDER = 'local_dev_trusted';

function isLoopbackUrl(value: string | undefined): boolean {
	if (!value) return false;

	try {
		return isLoopbackHostname(new URL(value).hostname);
	} catch {
		return false;
	}
}

function isLoopbackHostname(hostname: string | undefined): boolean {
	if (!hostname) return false;

	const normalized = hostname.toLowerCase();
	return (
		normalized === 'localhost' ||
		normalized === '127.0.0.1' ||
		normalized === '[::1]' ||
		normalized === '::1'
	);
}

function hostnameFromHostHeader(host: string | null): string | undefined {
	if (!host) return undefined;

	try {
		return new URL(`http://${host}`).hostname;
	} catch {
		return undefined;
	}
}

function isLoopbackClientAddress(address: string): boolean {
	const normalized = address.toLowerCase();

	if (normalized === '::1') return true;

	if (normalized.startsWith('::ffff:')) {
		return isLoopbackClientAddress(normalized.slice('::ffff:'.length));
	}

	if (isIP(normalized) === 4) {
		return normalized.startsWith('127.');
	}

	return false;
}

export function localDevTrustedAuthEnabled(): boolean {
	const runtimeEnv = currentWebRuntimeEnv();

	return (
		runtimeEnv.FAVN_WEB_LOCAL_DEV_TRUSTED_AUTH === '1' &&
		isLoopbackUrl(runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN) &&
		isLoopbackUrl(runtimeEnv.FAVN_WEB_ORCHESTRATOR_BASE_URL)
	);
}

export function localDevTrustedAuthAllowedForRequest(event: RequestEvent): boolean {
	if (!localDevTrustedAuthEnabled()) return false;

	const requestHostname =
		hostnameFromHostHeader(event.request.headers.get('host')) ?? event.url.hostname;
	if (!isLoopbackHostname(requestHostname)) return false;

	try {
		return isLoopbackClientAddress(event.getClientAddress());
	} catch {
		return false;
	}
}

export function localDevTrustedWebSession(): WebSession {
	return {
		session_token: '',
		session_id: 'local-dev-cli',
		actor_id: 'local-dev-cli',
		provider: LOCAL_DEV_TRUSTED_PROVIDER,
		expires_at: null,
		issued_at: null
	};
}

export function isLocalDevTrustedWebSession(session: WebSession): boolean {
	return session.provider === LOCAL_DEV_TRUSTED_PROVIDER;
}

export function applyLocalDevTrustedAuthHeader(headers: Headers): void {
	if (localDevTrustedAuthEnabled()) {
		headers.set(LOCAL_DEV_TRUSTED_AUTH_HEADER, LOCAL_DEV_TRUSTED_AUTH_HEADER_VALUE);
	}
}
