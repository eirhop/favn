<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, fn, within } from 'storybook/test';
	import AssetDetailPage from './AssetDetailPage.svelte';
	import {
		assetWithUnavailableScopes,
		failedAssetWithLatestRun,
		successfulAssetWithRuns
	} from './asset_detail_story_fixtures';

	const { Story } = defineMeta({
		title: 'Favn/Asset Catalog/Asset Detail Page',
		component: AssetDetailPage,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story
	name="Successful asset with runs"
	args={{ data: successfulAssetWithRuns, onrun: fn() }}
	play={async ({ canvasElement, userEvent, args }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('heading', { name: 'Customer revenue' })).toBeInTheDocument();
		await expect(canvas.getByText('SOURCE_SYSTEM_TOKEN')).toBeInTheDocument();
		await expect(canvas.getAllByText('declared')[0]).toBeInTheDocument();
		await expect(canvas.getByText('required · secret')).toBeInTheDocument();
		await expect(
			canvas.getByRole('heading', { name: 'Latest materialization' })
		).toBeInTheDocument();
		await expect(canvas.getByText('42 / —')).toBeInTheDocument();
		await userEvent.click(canvas.getByRole('tab', { name: 'Lineage' }));
		await expect(canvas.getByText('MyApp.Assets.Raw.Customers')).toBeInTheDocument();
		await userEvent.click(canvas.getByRole('tab', { name: 'Runs' }));
		await expect(canvas.getAllByRole('link', { name: 'Inspect' })[0]).toHaveAttribute(
			'href',
			'/runs/run_01HAPPY'
		);
		await userEvent.click(canvas.getByRole('button', { name: 'Run with dependencies' }));
		await expect(canvas.getByRole('dialog')).toHaveTextContent('mfv_2026_04_27');
		await expect(canvas.getByRole('dialog')).toHaveTextContent('With dependencies');
		await userEvent.click(canvas.getByRole('button', { name: 'Submit run request' }));
		await expect(args.onrun).toHaveBeenCalledWith({
			scope: 'with_dependencies',
			assetRef: 'MyApp.Assets.Mart.CustomerRevenue',
			manifestVersionId: 'mfv_2026_04_27'
		});
	}}
/>

<Story
	name="Failed asset with failed latest run"
	args={{ data: failedAssetWithLatestRun, onrun: fn() }}
	play={async ({ canvasElement, userEvent }) => {
		const canvas = within(canvasElement);
		await expect(
			canvas.getByRole('heading', { name: 'Customer orders staging' })
		).toBeInTheDocument();
		await expect(canvas.getAllByText('failed')[0]).toBeInTheDocument();
		await userEvent.click(canvas.getByRole('tab', { name: 'Runs' }));
		await expect(canvas.getByText('run_01FAILED')).toBeInTheDocument();
	}}
/>

<Story
	name="Unavailable asset-only and dependencies empty state"
	args={{ data: assetWithUnavailableScopes, onrun: fn() }}
	play={async ({ canvasElement, userEvent }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('button', { name: 'Asset-only run unavailable' })).toBeDisabled();
		await userEvent.click(canvas.getByRole('tab', { name: 'Lineage' }));
		await expect(
			canvas.getByText('Dependencies are not exposed by this asset detail payload yet.')
		).toBeInTheDocument();
		await userEvent.click(canvas.getByRole('tab', { name: 'Overview' }));
		await expect(canvas.getByText('No schema metadata was reported')).toBeInTheDocument();
	}}
/>
