<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{ children?: import('svelte').Snippet; class?: string; [key: string]: unknown }>();
</script>

<th class={cn('h-10 px-2 text-left align-middle font-medium text-slate-500', className)} {...rest}
	>{@render children?.()}</th
>
