<script setup lang="ts">

import Button from '@/components/Button.vue'
import { RouterLink } from 'vue-router'
import IconMonitoring from '@/components/icons/IconMonitoring.vue'
import IconPlan from '@/components/icons/IconPlan.vue'
</script>

<template>
  <header class="bg-blue-200 px-4 p-2 dark:bg-dark">
    <nav class="grid grid-cols-3 items-center justify-between">
      <img alt="DataTracks Logo" class="logo justify-self-start" src="@/assets/logo.png" width="50" height="50" />
      <h1 class="text-3xl font-bold justify-self-center">DataTracks</h1>
      <div class="flex gap-4 justify-self-end">
        <RouterLink class="cursor-pointer" to="/">
          <IconMonitoring />
        </RouterLink>
        <RouterLink class="cursor-pointer" to="/plan">
          <div class="cursor-pointer">
            <IconPlan />
          </div>
        </RouterLink>
      </div>
    </nav>
  </header>
</template>

<style scoped>

</style>