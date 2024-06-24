<script setup lang="ts">
defineProps<{
  title?: string
}>()
</script>

<template>
  <div class="container p-3 mx-auto">
    <h1 class="text-3xl font-bold mb-4" v-if="title">{{ title }}</h1>
    <slot />
  </div>
</template>

<style scoped>

</style>