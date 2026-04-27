<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import RunDetailPage from './RunDetailPage.svelte';
	import { failedRunDetail, realPayloadRunDetail } from './story_fixtures';

	const { Story } = defineMeta({
		title: 'Favn/Run Inspector/Run Detail Page',
		component: RunDetailPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story
	name="Failed Overview"
	args={{ run: failedRunDetail }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('heading', { name: 'Run details' })).toBeInTheDocument();
		await expect(canvas.getByText('run_01JABCD12')).toBeInTheDocument();
		await expect(
			canvas.getByText('Run failed in asset Staging.CustomerOrders')
		).toBeInTheDocument();
	}}
/>

<Story
	name="Events Tab Interaction"
	args={{ run: failedRunDetail }}
	play={async ({ canvasElement, userEvent }) => {
		const canvas = within(canvasElement);
		await userEvent.click(canvas.getByRole('button', { name: 'Events' }));
		await expect(canvas.getByText('asset_failed')).toBeInTheDocument();
	}}
/>

<Story
	name="Real Payload No Outputs"
	args={{ run: realPayloadRunDetail }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getAllByText(/ReferenceWorkloadComplete/).length).toBeGreaterThan(0);
		await expect(
			canvas.getByText('No materialized outputs reported by this run.')
		).toBeInTheDocument();
	}}
/>
