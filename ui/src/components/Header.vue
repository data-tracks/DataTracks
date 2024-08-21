<script setup lang="ts">
import { RouterLink } from 'vue-router'
import IconMonitoring from '@/components/icons/IconMonitoring.vue'
import IconPlan from '@/components/icons/IconPlan.vue'
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

const themeStore = useThemeStore()
const { isDark } = storeToRefs(themeStore)
</script>

<template>
  <header class="bg-gray-100 px-4 p-2 border border-gray-300" :class="{'bg-gray-900 border-gray-700': isDark}">
    <nav class="flex flex-col h-full items-center">
      <img alt="DataTracks Logo" class="logo mb-4" src="@/assets/logo.png" width="50" height="50" />

      <div class="flex gap-4 justify-self-center flex-col w-full">
        <div class="dark:hover:bg-gray-700 m-auto px-2 py-0.5 rounded">
          <RouterLink class="cursor-pointer" to="/">
            <IconMonitoring size="30px" :fill="isDark ? 'white' : 'black' " />
          </RouterLink>
        </div>
        <div class="dark:hover:bg-gray-700 m-auto px-2 py-0.5 rounded">
          <RouterLink class="cursor-pointer" to="/plan">
            <IconPlan size="30px" :fill="isDark ? 'white' : 'black' " />
          </RouterLink>
        </div>
      </div>
    </nav>
  </header>
</template>

<style scoped>

</style>
