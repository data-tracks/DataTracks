<script setup lang="ts">
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

withDefaults(defineProps<{
  text: string,
  disabled?: boolean
}>(), {
  disabled: false
})


const theme = useThemeStore()
const { isDark } = storeToRefs(theme)
</script>

<template>
  <div class="bg-blue-400 text-gray-200 hover:cursor-pointer rounded p-1 px-3 inline-block"
       :class="{'pointer-events-none bg-gray-200 text-neutral-50': disabled && !isDark, 'text-neutral-800' : isDark && !disabled, 'text-neutral-400 bg-neutral-700': isDark && disabled }">
    {{ text }}
  </div>
</template>

<style scoped>

</style>
