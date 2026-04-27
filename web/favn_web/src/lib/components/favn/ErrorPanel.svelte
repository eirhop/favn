<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import * as Alert from '$lib/components/ui/alert';

	let { asset, message, oninspect } = $props<{
		asset: string;
		message: string;
		oninspect?: () => void;
	}>();
</script>

<Alert.Root variant="destructive" class="bg-red-50">
	<p class="mb-1 font-medium tracking-tight">Run failed in asset {asset}</p>
	<Alert.Description>{message}</Alert.Description>
	<div class="mt-3 flex flex-wrap gap-2">
		<Button size="sm" variant="outline" onclick={() => oninspect?.()}>Inspect failed asset</Button>
		<Button size="sm" variant="outline" onclick={() => navigator.clipboard?.writeText(message)}>
			Copy error
		</Button>
	</div>
</Alert.Root>
