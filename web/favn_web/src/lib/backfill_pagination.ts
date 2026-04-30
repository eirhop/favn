import type { PaginationView } from '$lib/backfill_view_types';

export type PaginationLinks = {
	currentOffset: number;
	limit: number | null;
	previousHref: string | null;
	nextHref: string | null;
};

function finiteInteger(value: number | null): number | null {
	return typeof value === 'number' && Number.isFinite(value)
		? Math.max(0, Math.trunc(value))
		: null;
}

export function buildPageHref(
	pathname: string,
	searchParams: URLSearchParams,
	limit: number,
	offset: number
) {
	const params = new URLSearchParams(searchParams);
	params.set('limit', String(limit));
	params.set('offset', String(Math.max(0, offset)));
	const query = params.toString();
	return query ? `${pathname}?${query}` : pathname;
}

export function buildPaginationLinks(
	pathname: string,
	searchParams: URLSearchParams,
	pagination: PaginationView
): PaginationLinks {
	const limit = finiteInteger(pagination.limit);
	const currentOffset = finiteInteger(pagination.offset) ?? 0;

	return {
		currentOffset,
		limit,
		previousHref:
			pagination.hasPrevious && limit !== null
				? buildPageHref(pathname, searchParams, limit, Math.max(0, currentOffset - limit))
				: null,
		nextHref:
			pagination.hasNext && limit !== null
				? buildPageHref(pathname, searchParams, limit, currentOffset + limit)
				: null
	};
}
