<script setup lang="ts">
defineProps<{
  isValid: boolean,
  message?: string
}>()

</script>


<template>
<div class="flex flex-col" :class="isValid ? 'border-gray-500' : 'border-red-300'">
  <slot />
</div>
  <span v-if="!isValid && message">{{ message }}</span>
</template>

<style scoped>

</style>
