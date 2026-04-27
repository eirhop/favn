import { describe, expect, it } from 'vitest';
import { load } from './+page.server';

describe('root page server load', () => {
	it('redirects to the run inspector landing page', async () => {
		await expect(load({} as never)).rejects.toMatchObject({ status: 303, location: '/runs' });
	});
});
