<script setup lang="ts">
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'
import Status from '@/components/Status.vue'

defineProps<{
  title?: string
}>()

const themeStore = useThemeStore()

const { isDark } = storeToRefs(themeStore)
</script>

<template>
  <div class="container mx-auto p-6">
    <div class="flex flex-wrap justify-between items-center mb-4 px-2">
      <h1 class="text-3xl font-bold" :class="{'text-gray-100' : isDark}" v-if="title">{{ title }}</h1>
      <Status />
    </div>

    <slot />
  </div>
</template>

<style scoped>

</style>
