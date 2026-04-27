<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import DashboardPage from './DashboardPage.svelte';

	const { Story } = defineMeta({
		title: 'Favn/DashboardPage',
		component: DashboardPage,
		parameters: { layout: 'fullscreen' }
	});

	const baseArgs = {
		session: { actor_id: 'actor_alice', provider: 'password_local' },
		activeManifestVersionId: 'manifest_v2',
		runs: [
			{ id: 'run_001', status: 'succeeded', target: 'asset:asset.orders' },
			{ id: 'run_002', status: 'running', target: 'pipeline:pipeline.reconcile' }
		],
		schedules: [
			{ id: 'sched_001', enabled: true, target: 'asset:asset.orders' },
			{ id: 'sched_002', enabled: false, target: 'pipeline:pipeline.reconcile' }
		],
		orchestratorWarning: null
	};
</script>

<Story
	name="Populated"
	args={baseArgs}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('heading', { name: 'Favn web prototype' })).toBeInTheDocument();
		await expect(canvas.getByRole('row', { name: /run_001/ })).toBeInTheDocument();
		await expect(canvas.getByText('manifest_v2')).toBeInTheDocument();
	}}
/>

<Story
	name="Local Admin Warning"
	args={{
		...baseArgs,
		session: { actor_id: 'admin:local', provider: 'web_local_admin' },
		runs: [],
		schedules: [],
		activeManifestVersionId: null,
		orchestratorWarning: 'Signed in with web-local admin credentials.'
	}}
/>
