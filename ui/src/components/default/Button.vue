<script setup lang="ts">
defineProps<{
  text: string,
  disabled?: boolean
}>()
</script>

<template>
  <div class="bg-blue-200 border-black border cursor-pointer rounded p-1 px-3 inline-block" :class="disabled ? 'pointer-events-none bg-gray-400' : 'hover:bg-blue-300'">
    {{ text }}
  </div>
</template>

<style scoped>

</style>