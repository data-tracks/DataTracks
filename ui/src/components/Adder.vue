<script setup lang="ts">
import type { Addable } from '@/stores/options'

const props = defineProps<{
  adds: Addable[]
}>()
</script>

<template>
  <div v-for="add in adds" :key="add.name" @click="add.onAdd(add.name, add.configs)">
    {{add.name}}
    {{add.configs}}
  </div>
</template>

<style scoped>

</style>
