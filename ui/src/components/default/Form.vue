<script setup lang="ts">
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

defineProps<{
  isValid: boolean,
  message?: string
}>()

const themeStore = useThemeStore();

const {isDark} = storeToRefs(themeStore);

</script>


<template>
<div class="flex flex-col text-black" :class="isValid ? 'border-gray-500' : 'border-red-300'">
  <slot />
</div>
  <span v-if="!isValid && message">{{ message }}</span>
</template>

<style scoped>

</style>
