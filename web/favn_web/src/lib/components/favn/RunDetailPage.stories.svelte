<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import RunDetailPage from './RunDetailPage.svelte';
	import { failedRunDetail } from './story_fixtures';

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
		await expect(
			canvas.getByRole('heading', { name: `Run ${failedRunDetail.id}` })
		).toBeInTheDocument();
		await expect(
			canvas.getByText('Run failed in asset Staging.CustomerOrders')
		).toBeInTheDocument();
	}}
/>

<Story
	name="Outputs Tab Interaction"
	args={{ run: failedRunDetail }}
	play={async ({ canvasElement, userEvent }) => {
		const canvas = within(canvasElement);
		await userEvent.click(canvas.getByRole('button', { name: 'Outputs' }));
		await expect(canvas.getByText('staging.customer_orders')).toBeInTheDocument();
	}}
/>
