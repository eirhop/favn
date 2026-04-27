<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import { expect, within } from 'storybook/test';
	import LoginPanel from './LoginPanel.svelte';

	const { Story } = defineMeta({
		title: 'Favn/LoginPanel',
		component: LoginPanel,
		parameters: { layout: 'fullscreen' }
	});
</script>

<Story
	name="Configured"
	args={{ localAdminConfigured: true, form: null }}
	play={async ({ canvasElement }) => {
		const canvas = within(canvasElement);
		await expect(canvas.getByRole('heading', { name: 'Admin sign in' })).toBeInTheDocument();
		await expect(canvas.getByLabelText('Username')).toBeInTheDocument();
		await expect(canvas.getByRole('button', { name: 'Log in' })).toBeInTheDocument();
	}}
/>

<Story
	name="With Error"
	args={{
		localAdminConfigured: false,
		form: { message: 'Invalid username or password', username: 'operator' }
	}}
/>
