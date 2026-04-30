import { describe, expect, it } from 'vitest';
import { buildPaginationLinks } from './backfill_pagination';

describe('buildPaginationLinks', () => {
	it('preserves filters while moving by limit', () => {
		const links = buildPaginationLinks(
			'/backfills/bf_001',
			new URLSearchParams('status=failed&limit=50&offset=50'),
			{ limit: 50, offset: 50, total: 150, hasNext: true, hasPrevious: true }
		);

		expect(links.previousHref).toBe('/backfills/bf_001?status=failed&limit=50&offset=0');
		expect(links.nextHref).toBe('/backfills/bf_001?status=failed&limit=50&offset=100');
	});

	it('omits unavailable links', () => {
		const links = buildPaginationLinks('/assets/window-states', new URLSearchParams(), {
			limit: 50,
			offset: 0,
			total: 20,
			hasNext: false,
			hasPrevious: false
		});

		expect(links.previousHref).toBeNull();
		expect(links.nextHref).toBeNull();
	});
});
