<script setup lang="ts">
import type { Addable } from '@/stores/options'

const props = defineProps<{
  adds: Addable[]
}>()
</script>

<template>
  <div v-for="add in adds" :key="add.name">
    {{add.name}}
  </div>
</template>

<style scoped>

</style>
