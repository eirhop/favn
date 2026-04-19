import { spawn } from 'node:child_process';

const HOST = '127.0.0.1';
const ORCHESTRATOR_PORT = Number(process.env.FAVN_ORCHESTRATOR_PORT ?? 4101);
const PREVIEW_PORT = Number(process.env.FAVN_WEB_PREVIEW_PORT ?? 4173);

let shuttingDown = false;
let shutdownSignal = 'SIGTERM';
let expectedExitCode = 0;

function spawnChild(label, command, args, extraEnv = {}) {
	const child = spawn(command, args, {
		stdio: 'inherit',
		env: {
			...process.env,
			...extraEnv
		}
	});

	child.on('error', (error) => {
		console.error(`[e2e-stack] failed to start ${label}:`, error);
		shutdown(1, 'SIGTERM');
	});

	return child;
}

const mockServer = spawnChild('mock orchestrator', 'node', ['./tests/e2e/mock-orchestrator-server.mjs'], {
	FAVN_ORCHESTRATOR_PORT: String(ORCHESTRATOR_PORT)
});

const previewServer = spawnChild(
	'svelte preview',
	'npm',
	['run', 'preview', '--', '--host', HOST, '--port', String(PREVIEW_PORT)]
);

function handleUnexpectedExit(name, code, signal) {
	if (shuttingDown) {
		return;
	}

	console.error(
		`[e2e-stack] ${name} exited unexpectedly (code=${code ?? 'null'}, signal=${signal ?? 'null'})`
	);
	shutdown(1, 'SIGTERM');
}

mockServer.on('exit', (code, signal) => handleUnexpectedExit('mock orchestrator', code, signal));
previewServer.on('exit', (code, signal) => handleUnexpectedExit('svelte preview', code, signal));

function terminateChild(child, signal) {
	if (!child || child.killed || child.exitCode !== null || child.signalCode !== null) {
		return;
	}

	child.kill(signal);

	setTimeout(() => {
		if (child.exitCode === null && child.signalCode === null) {
			child.kill('SIGKILL');
		}
	}, 3_000).unref();
}

function waitForExit(child) {
	if (child.exitCode !== null || child.signalCode !== null) {
		return Promise.resolve();
	}

	return new Promise((resolve) => child.once('exit', resolve));
}

function shutdown(code = 0, signal = 'SIGTERM') {
	if (shuttingDown) {
		return;
	}

	shuttingDown = true;
	expectedExitCode = code;
	shutdownSignal = signal;

	terminateChild(previewServer, signal);
	terminateChild(mockServer, signal);

	Promise.allSettled([waitForExit(previewServer), waitForExit(mockServer)]).finally(() => {
		process.exit(expectedExitCode);
	});
}

process.on('SIGINT', () => shutdown(0, 'SIGINT'));
process.on('SIGTERM', () => shutdown(0, 'SIGTERM'));
process.on('uncaughtException', (error) => {
	console.error('[e2e-stack] uncaught exception', error);
	shutdown(1, 'SIGTERM');
});

process.on('unhandledRejection', (error) => {
	console.error('[e2e-stack] unhandled rejection', error);
	shutdown(1, 'SIGTERM');
});

process.on('exit', () => {
	if (!shuttingDown) {
		terminateChild(previewServer, shutdownSignal);
		terminateChild(mockServer, shutdownSignal);
	}
});
