<script setup lang="ts">
import { RouterLink } from 'vue-router'
import IconMonitoring from '@/components/icons/IconMonitoring.vue'
import IconPlan from '@/components/icons/IconPlan.vue'
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'
import router from '@/router'
import type { Component } from 'vue'

const themeStore = useThemeStore()
const { isDark } = storeToRefs(themeStore)

const routes = router.getRoutes()

const getComponent = (path: String): Component => {
  switch (path) {
    case '/':
      return IconMonitoring;
    case '/plan':
      return IconPlan;
    default:
      return IconMonitoring;
  }
}

</script>

<template>
  <header class="bg-gray-100 px-4 p-2 border border-gray-300" :class="{'bg-gray-900 border-gray-700': isDark}">
    <nav class="flex flex-col h-full items-center">
      <img alt="DataTracks Logo" class="logo mb-4" src="@/assets/logo.png" width="50" height="50" />

      <div class="flex gap-4 justify-self-center flex-col w-full">
        <div class="m-auto px-2 py-0.5 rounded hover:bg-gray-400" :class="{'hover:bg-gray-700':isDark}"
             v-for="route in routes " :key="route.path">
          <RouterLink class="cursor-pointer" :to="route.path">
            <component :is="getComponent(route.path)" v-bind="{ size: '30px', fill: isDark ? 'white' : 'black'}" />
          </RouterLink>
        </div>
      </div>
    </nav>
  </header>
</template>

<style scoped>

</style>
