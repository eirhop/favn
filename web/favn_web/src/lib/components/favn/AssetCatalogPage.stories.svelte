<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, userEvent, within } from 'storybook/test';
	import AssetCatalogPage from './AssetCatalogPage.svelte';
	import { mixedAssetCatalog, noActiveManifestCatalog } from './asset_catalog_story_fixtures';

	const { Story } = defineMeta({
		title: 'Favn/Asset Catalog/Page',
		component: AssetCatalogPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story
	name="Default mixed catalog"
	args={{ catalog: mixedAssetCatalog }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('heading', { name: 'Assets' })).toBeInTheDocument();
		await expect(canvas.getByText('Customer profiles')).toBeInTheDocument();

		await userEvent.type(canvas.getByLabelText('Search assets'), 'finance');
		await expect(
			canvas.getByText('Order line items enriched for finance controls')
		).toBeInTheDocument();
		await expect(canvas.queryByText('Customer profiles')).not.toBeInTheDocument();

		await userEvent.clear(canvas.getByLabelText('Search assets'));
		await userEvent.selectOptions(canvas.getByLabelText('Status'), 'failed');
		await expect(
			canvas.getByText('Order line items enriched for finance controls')
		).toBeInTheDocument();
		await expect(canvas.queryByText('Rebuild safety stock levels')).not.toBeInTheDocument();

		await userEvent.selectOptions(canvas.getByLabelText('Status'), 'all');
		await userEvent.click(canvas.getByRole('button', { name: /Never run \/ Unknown/ }));
		await expect(canvas.getByText('Ad spend snapshot')).toBeInTheDocument();
		await expect(canvas.queryByText('Customer profiles')).not.toBeInTheDocument();
	}}
/>

<Story
	name="No active manifest"
	args={{ catalog: noActiveManifestCatalog }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByText('No active manifest')).toBeInTheDocument();
		await expect(canvas.getByText(/Publish or activate a manifest/)).toBeInTheDocument();
	}}
/>

<Story
	name="No search results"
	args={{ catalog: mixedAssetCatalog }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await userEvent.type(canvas.getByLabelText('Search assets'), 'does-not-exist');
		await expect(canvas.getByText('No assets match these filters')).toBeInTheDocument();
		await userEvent.click(canvas.getAllByRole('button', { name: 'Clear filters' }).at(-1)!);
		await expect(canvas.getByText('Customer profiles')).toBeInTheDocument();
	}}
/>
