<script setup lang="ts">

import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

export interface CardProps {
  hasPadding?: boolean,
}

withDefaults(defineProps<CardProps>(), {
  hasPadding: false,
});

const themeStore = useThemeStore();
const {isDark} = storeToRefs(themeStore);

</script>

<template>
  <div class="pb-4 border border-gray-300 rounded-md shadow" :class="{'border-gray-900 shadow-gray-500 text-gray-100 bg-gray-900': isDark}">
    <div class="p-2 mb-4 px-4 flex justify-between border-b border-gray-300" v-if="$slots.left || $slots.right">
      <div class="font-medium">
        <slot name="left"></slot>
      </div>

      <div class="font-medium">
        <slot name="right"></slot>
      </div>
    </div>
    <div :class="hasPadding ? 'p-4': ''">
      <slot />
    </div>

  </div>
</template>

<style scoped>

</style>
