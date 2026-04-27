<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import RunInspectorComponentGallery from './RunInspectorComponentGallery.svelte';

	const { Story } = defineMeta({
		title: 'Favn/Run Inspector/Components',
		component: RunInspectorComponentGallery,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story
	name="Status Badges"
	args={{ variant: 'status-badges' }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByText('running')).toBeInTheDocument();
		await expect(canvas.getByText('failed')).toBeInTheDocument();
	}}
/>

<Story
	name="Runs Table"
	args={{ variant: 'runs-table' }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('row', { name: /run_01JABCD12/ })).toBeInTheDocument();
	}}
/>

<Story name="Summary Cards" args={{ variant: 'summary-cards' }} />
<Story name="Error Panel" args={{ variant: 'error-panel' }} />
<Story name="Asset Table" args={{ variant: 'asset-table' }} />
<Story name="Outputs Table" args={{ variant: 'outputs-table' }} />
<Story name="Timeline" args={{ variant: 'timeline' }} />
<Story name="Manifest Card" args={{ variant: 'manifest-card' }} />
<Story name="Asset Detail Sheet" args={{ variant: 'asset-sheet' }} />
