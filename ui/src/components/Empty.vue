<script setup lang="ts">

const props = withDefaults(defineProps<{
  text: string
}>(), {
  text: "Test"
})

</script>

<template>
<div>
  <h1>{{ text }}</h1>
</div>
</template>

<style scoped>

</style>
