<script lang="ts">
	import { cn } from '$lib/utils';
	type Variant = 'default' | 'destructive';
	let {
		children,
		class: className,
		variant = 'default',
		...rest
	} = $props<{
		children?: import('svelte').Snippet;
		class?: string;
		variant?: Variant;
		[key: string]: unknown;
	}>();
</script>

<div
	class={cn(
		'relative w-full rounded-lg border p-4 text-sm',
		variant === 'destructive' ? 'border-red-200 text-red-800' : 'bg-white text-slate-950',
		className
	)}
	role="alert"
	{...rest}
>
	{@render children?.()}
</div>
