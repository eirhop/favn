<script lang="ts">
	import { resolve } from '$app/paths';
	import { cn } from '$lib/utils';

	type Variant = 'default' | 'destructive' | 'outline' | 'secondary' | 'ghost' | 'link';
	type Size = 'default' | 'sm' | 'lg' | 'icon' | 'icon-sm' | 'icon-lg';

	let {
		children,
		class: className,
		variant = 'default',
		size = 'default',
		href,
		type = 'button',
		...rest
	} = $props<{
		children?: import('svelte').Snippet;
		class?: string;
		variant?: Variant;
		size?: Size;
		href?: string;
		type?: 'button' | 'submit' | 'reset';
		[key: string]: unknown;
	}>();

	const variants: Record<Variant, string> = {
		default: 'bg-slate-950 text-white shadow hover:bg-slate-800',
		destructive: 'bg-red-600 text-white shadow-sm hover:bg-red-700',
		outline: 'border border-slate-200 bg-white shadow-sm hover:bg-slate-100 hover:text-slate-900',
		secondary: 'bg-slate-100 text-slate-900 shadow-sm hover:bg-slate-200',
		ghost: 'hover:bg-slate-100 hover:text-slate-900',
		link: 'text-slate-950 underline-offset-4 hover:underline shadow-none'
	};

	const sizes: Record<Size, string> = {
		default: 'h-9 px-4 py-2',
		sm: 'h-8 rounded-md px-3 text-xs',
		lg: 'h-10 rounded-md px-8',
		icon: 'size-9',
		'icon-sm': 'size-8',
		'icon-lg': 'size-10'
	};

	let buttonClass = $derived(
		cn(
			'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-slate-950 disabled:pointer-events-none disabled:opacity-50',
			variants[variant as Variant],
			sizes[size as Size],
			className
		)
	);
</script>

{#if href}
	<a class={buttonClass} href={resolve(href)} {...rest}>{@render children?.()}</a>
{:else}
	<button class={buttonClass} {type} {...rest}>{@render children?.()}</button>
{/if}
