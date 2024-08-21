<script setup lang="ts">
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

defineProps<{
  title?: string
}>()

const themeStore = useThemeStore();

const {isDark} = storeToRefs(themeStore);
</script>

<template>
  <div class="container p-3 mx-auto">
    <h1 class="text-3xl font-bold mb-4" :class="{'text-gray-100' : isDark}" v-if="title">{{ title }}</h1>
    <slot />
  </div>
</template>

<style scoped>

</style>
