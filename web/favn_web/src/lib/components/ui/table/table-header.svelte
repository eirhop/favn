<script lang="ts">
	let { children, ...rest } = $props<{
		children?: import('svelte').Snippet;
		[key: string]: unknown;
	}>();
</script>

<thead {...rest}>{@render children?.()}</thead>
