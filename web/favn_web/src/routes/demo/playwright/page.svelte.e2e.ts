import { expect, test } from '@playwright/test';

test('demo route is protected by hook-level auth', async ({ page }) => {
	await page.goto('/demo/playwright');
	await expect(page).toHaveURL(/\/login\?next=%2Fdemo%2Fplaywright/);
	await expect(page.getByRole('heading', { name: 'Login' })).toBeVisible();
});
