const sanitizedResponses = new WeakSet<Response>();

export function markSanitizedResponse(response: Response): Response {
	sanitizedResponses.add(response);
	return response;
}

export function isSanitizedResponse(response: Response): boolean {
	return sanitizedResponses.has(response);
}
