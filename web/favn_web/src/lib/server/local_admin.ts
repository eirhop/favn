import { env } from '$env/dynamic/private';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import type { WebSession } from './session';

const DEFAULT_ADMIN_SESSION_TTL_SECONDS = 8 * 60 * 60;

function configuredValue(name: string): string | null {
	const value = process.env[name] ?? env[name];
	return typeof value === 'string' && value.length > 0 ? value : null;
}

function constantTimeEquals(actual: string, expected: string): boolean {
	const actualBuffer = Buffer.from(actual, 'utf8');
	const expectedBuffer = Buffer.from(expected, 'utf8');

	if (actualBuffer.length !== expectedBuffer.length) return false;
	return timingSafeEqual(actualBuffer, expectedBuffer);
}

function adminSessionTtlSeconds(): number {
	const configured = Number(
		process.env.FAVN_WEB_ADMIN_SESSION_TTL_SECONDS ?? env.FAVN_WEB_ADMIN_SESSION_TTL_SECONDS
	);
	return Number.isFinite(configured) && configured > 0
		? configured
		: DEFAULT_ADMIN_SESSION_TTL_SECONDS;
}

export function localAdminLogin(username: string, password: string): WebSession | null {
	const configuredUsername = configuredValue('FAVN_WEB_ADMIN_USERNAME');
	const configuredPassword = configuredValue('FAVN_WEB_ADMIN_PASSWORD');

	if (!configuredUsername || !configuredPassword) return null;
	if (!constantTimeEquals(username, configuredUsername)) return null;
	if (!constantTimeEquals(password, configuredPassword)) return null;

	const issuedAt = new Date();
	const expiresAt = new Date(issuedAt.getTime() + adminSessionTtlSeconds() * 1000);

	return {
		session_id: `web_local_admin_${randomUUID()}`,
		actor_id: `admin:${configuredUsername}`,
		provider: 'web_local_admin',
		issued_at: issuedAt.toISOString(),
		expires_at: expiresAt.toISOString()
	};
}

export function localAdminConfigured(): boolean {
	return Boolean(
		configuredValue('FAVN_WEB_ADMIN_USERNAME') && configuredValue('FAVN_WEB_ADMIN_PASSWORD')
	);
}
