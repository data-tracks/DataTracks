<script setup lang="ts">
defineProps<{
  isValid: boolean,
  message?: string
}>()
</script>

<template>
<div class="border rounded flex flex-col" :class="isValid ? 'border-black' : 'border-red-500 border-2'">
  <slot />
</div>
  <span v-if="!isValid && message">{{ message }}</span>
</template>

<style scoped>

</style>