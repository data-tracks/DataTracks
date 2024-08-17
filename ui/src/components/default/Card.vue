<script setup lang="ts">

</script>

<template>
  <div class="bg-white pb-4 dark:bg-dark drop-shadow-md border-2 rounded-md">
    <div class="bg-blue-200 p-2 mb-4 px-4 flex justify-between">
      <div class="font-medium">
        <slot name="left"></slot>
      </div>

      <div class="font-medium">
        <slot name="right"></slot>
      </div>
    </div>
    <slot />
  </div>
</template>

<style scoped>

</style>