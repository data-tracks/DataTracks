<script setup lang="ts">
defineProps<{
  text: string
}>()
</script>

<template>
<div class="bg-sky-500 hover:bg-sky-600 cursor-pointer rounded p-1 px-2 inline-block">{{text}}</div>
</template>

<style scoped>

</style>