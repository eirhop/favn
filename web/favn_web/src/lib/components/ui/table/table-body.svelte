<script lang="ts">
	let { children, ...rest } = $props<{
		children?: import('svelte').Snippet;
		[key: string]: unknown;
	}>();
</script>

<tbody {...rest}>{@render children?.()}</tbody>
