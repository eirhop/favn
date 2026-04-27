<script lang="ts">
	import { cn } from '$lib/utils';
	let {
		children,
		class: className,
		...rest
	} = $props<{ children?: import('svelte').Snippet; class?: string; [key: string]: unknown }>();
</script>

<div class="relative w-full overflow-auto">
	<table class={cn('w-full caption-bottom text-sm', className)} {...rest}>
		{@render children?.()}
	</table>
</div>
