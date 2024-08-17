<script setup lang="ts">
import { RouterLink } from 'vue-router'
import IconMonitoring from '@/components/icons/IconMonitoring.vue'
import IconPlan from '@/components/icons/IconPlan.vue'
</script>

<template>
  <header class="bg-gray-100 px-4 p-2 dark:bg-dark drop-shadow-md">
    <nav class="grid grid-cols-3 items-center justify-between">
      <img alt="DataTracks Logo" class="logo justify-self-start" src="@/assets/logo.png" width="50" height="50" />
      <h1 class="text-3xl font-bold justify-self-center">DataTracks</h1>
      <div class="flex gap-4 justify-self-end">
        <RouterLink class="cursor-pointer grid grid-flow-col items-center" to="/">
          <IconMonitoring />
          <h2 class="ml-1">Monitoring</h2>
        </RouterLink>
        <RouterLink class="cursor-pointer" to="/plan">
          <div class="cursor-pointer grid grid-flow-col items-center" to="/">
            <IconPlan />
            <h2 class="ml-1">Manage</h2>
          </div>
        </RouterLink>
      </div>
    </nav>
  </header>
</template>

<style scoped>

</style>