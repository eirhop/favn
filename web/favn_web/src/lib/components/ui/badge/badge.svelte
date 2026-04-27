<script lang="ts">
	import { cn } from '$lib/utils';
	type Variant = 'default' | 'secondary' | 'destructive' | 'outline';
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
	const variants: Record<Variant, string> = {
		default: 'border-transparent bg-slate-950 text-white',
		secondary: 'border-transparent bg-slate-100 text-slate-900',
		destructive: 'border-transparent bg-red-600 text-white',
		outline: 'text-slate-950'
	};
</script>

<span
	class={cn(
		'inline-flex items-center rounded-md border px-2.5 py-0.5 text-xs font-semibold transition-colors',
		variants[variant as Variant],
		className
	)}
	{...rest}>{@render children?.()}</span
>
