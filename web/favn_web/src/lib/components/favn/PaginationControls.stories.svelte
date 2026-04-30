<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import PaginationControls from './PaginationControls.svelte';

	const { Story } = defineMeta({
		title: 'Favn/Backfills/Pagination Controls',
		component: PaginationControls
	});
</script>

<Story
	name="Middle Page"
	args={{ pagination: { limit: 50, offset: 50, total: 150, hasNext: true, hasPrevious: true } }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByText('Offset 50 · limit 50')).toBeInTheDocument();
		await expect(canvas.getByRole('link', { name: 'Previous' })).toBeInTheDocument();
		await expect(canvas.getByRole('link', { name: 'Next' })).toBeInTheDocument();
	}}
/>

<Story
	name="First Page"
	args={{ pagination: { limit: 50, offset: 0, total: 25, hasNext: false, hasPrevious: false } }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('button', { name: 'Previous' })).toBeDisabled();
		await expect(canvas.getByRole('button', { name: 'Next' })).toBeDisabled();
	}}
/>
