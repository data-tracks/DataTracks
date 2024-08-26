<script setup lang="ts">

import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

export interface CardProps {
  hasPadding?: boolean,
}

withDefaults(defineProps<CardProps>(), {
  hasPadding: false
})

const themeStore = useThemeStore()
const { isDark } = storeToRefs(themeStore)

</script>

<template>
  <div class="border border-gray-300 rounded-md shadow"
       :class="{'border-gray-900 shadow-gray-800 text-gray-100 bg-gray-900': isDark}">
    <div class="p-2 mb-4 px-4 flex justify-between border-b border-gray-300" v-if="$slots.left || $slots.right">
      <div class="font-medium">
        <slot name="left"></slot>
      </div>

      <div class="font-medium">
        <slot name="right"></slot>
      </div>
    </div>
    <div class="pb-6" :class="hasPadding ? 'p-4': ''">
      <slot />
    </div>
    <div v-if="$slots.bottom" class="border-t border-gray-300 py-4" :class="{'border-grey-900 bg-gray-800': isDark}">
      <slot name="bottom"></slot>
    </div>

  </div>
</template>

<style scoped>

</style>
