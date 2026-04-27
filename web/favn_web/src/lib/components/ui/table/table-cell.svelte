<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{
		children?: import('svelte').Snippet;
		class?: string;
		colspan?: number;
		[key: string]: unknown;
	}>();
</script>

<td class={cn('p-2 align-middle', className)} {...rest}>{@render children?.()}</td>
