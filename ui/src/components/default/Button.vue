<script setup lang="ts">
defineProps<{
  text: string,
  disabled?: boolean
}>()
</script>

<template>
  <div class="bg-blue-400 text-gray-500 hover:cursor-pointer rounded p-1 px-3 inline-block" :class="disabled ? 'pointer-events-none bg-gray-200 text-gray-500' : 'hover:bg-blue-300'">
    {{ text }}
  </div>
</template>

<style scoped>

</style>
