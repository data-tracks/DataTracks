<script setup lang="ts">

import Button from '@/components/Button.vue'
import { RouterLink } from 'vue-router'
import IconMonitoring from '@/components/icons/IconMonitoring.vue'
</script>

<template>
  <header class="bg-blue-200 px-4 dark:bg-dark">
    <nav class="flex items-center justify-between">
      <img alt="DataTracks Logo" class="logo" src="@/assets/logo.png" width="90" height="90" />
      <h1 class="text-3xl font-bold">DataTracks</h1>
      <div class="flex gap-1">
        <RouterLink to="/">
          <IconMonitoring />
        </RouterLink>
        <RouterLink to="/planning">
          <Button text="Planning" />
        </RouterLink>
      </div>
    </nav>
  </header>
</template>

<style scoped>

</style>