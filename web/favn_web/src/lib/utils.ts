// Local shadcn-style primitive for static/conditional class strings.
// This project does not currently depend on clsx/tailwind-merge, so keep usage
// to plain strings and falsey conditionals rather than conflict resolution.
export function cn(...inputs: Array<string | false | null | undefined>): string {
	return inputs.filter(Boolean).join(' ');
}
