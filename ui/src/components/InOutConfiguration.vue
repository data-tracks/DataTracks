<script setup lang="ts">
import type { Destination, Source, Stop } from '@/stores/plan'

const props = defineProps<{
  inOut: Source | Destination
}>()
</script>

<template>
  <div>
    {{inOut.type_name}}
  </div>
</template>

<style scoped>

</style>
