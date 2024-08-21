<script setup lang="ts">
import { Theme, useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

const themeStore = useThemeStore();

const {isDark} = storeToRefs(themeStore);

</script>

<template>
<div class="p-2 px-4 flex text-sm border-t border-gray-300  justify-between" :class="{'text-gray-100 border-gray-700 bg-gray-800': isDark }">
  @DataTracks {{new Date().getFullYear()}}
  <div class="flex gap-1 cursor-pointer">
    <p @click="themeStore.changeTheme(Theme.LIGHT)">☀️</p>
    <p>/</p>
    <p @click="themeStore.changeTheme(Theme.DARK)">🌑</p>
  </div>
</div>
</template>

<style scoped>

</style>
