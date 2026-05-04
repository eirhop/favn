import { currentWebRuntimeEnv } from './runtime_config';
import type { WebSession } from './session';

export const LOCAL_DEV_TRUSTED_AUTH_HEADER = 'x-favn-local-dev-context';
export const LOCAL_DEV_TRUSTED_AUTH_HEADER_VALUE = 'trusted';
export const LOCAL_DEV_TRUSTED_PROVIDER = 'local_dev_trusted';

function isLoopbackUrl(value: string | undefined): boolean {
	if (!value) return false;

	try {
		const hostname = new URL(value).hostname.toLowerCase();
		return (
			hostname === 'localhost' ||
			hostname === '127.0.0.1' ||
			hostname === '[::1]' ||
			hostname === '::1'
		);
	} catch {
		return false;
	}
}

export function localDevTrustedAuthEnabled(): boolean {
	const runtimeEnv = currentWebRuntimeEnv();

	return (
		runtimeEnv.FAVN_WEB_LOCAL_DEV_TRUSTED_AUTH === '1' &&
		isLoopbackUrl(runtimeEnv.FAVN_WEB_PUBLIC_ORIGIN) &&
		isLoopbackUrl(runtimeEnv.FAVN_WEB_ORCHESTRATOR_BASE_URL)
	);
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
