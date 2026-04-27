<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import RunsPage from './RunsPage.svelte';
	import { realPayloadRunSummary, sampleRuns } from './story_fixtures';

	const { Story } = defineMeta({
		title: 'Favn/Run Inspector/Runs Page',
		component: RunsPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story
	name="Populated"
	args={{ runs: [realPayloadRunSummary, ...sampleRuns], loadError: null }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('heading', { name: 'Runs' })).toBeInTheDocument();
		await expect(
			canvas.getByRole('row', { name: /ReferenceWorkloadComplete/ })
		).toBeInTheDocument();
		await expect(canvas.getByText('3.3s')).toBeInTheDocument();
	}}
/>

<Story
	name="Empty"
	args={{ runs: [], loadError: null }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByText('No runs yet')).toBeInTheDocument();
		await expect(canvas.getByText('mix favn.dev')).toBeInTheDocument();
	}}
/>

<Story name="Load Error" args={{ runs: [], loadError: 'HTTP 502' }} />
