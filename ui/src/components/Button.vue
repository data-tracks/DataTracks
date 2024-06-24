<script setup lang="ts">
defineProps<{
  text: string
}>()
</script>

<template>
  <div class="bg-blue-200 hover:bg-blue-300 border-black border cursor-pointer rounded p-1 px-3 inline-block">
    {{ text }}
  </div>
</template>

<style scoped>

</style>