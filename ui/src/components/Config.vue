<script setup lang="ts">
import { ConfigModel } from '@/stores/plan'

const props = defineProps<{
  key_: string,
  config: ConfigModel
}>()
</script>

<template>
  <div
    class="flex mt-2 rounded-md shadow-sm ring-1 ring-inset ring-gray-300 focus-within:ring-2 focus-within:ring-inset focus-within:ring-indigo-600 sm:max-w-md">
    <span class="flex select-none items-center pl-3 text-gray-500 sm:text-sm">{{ key_ }}</span>
    <input type="text" name="key" id="key" autocomplete="username"
           class="block flex-1 border-0 bg-transparent py-1.5 pl-1 placeholder:text-gray-400 focus:ring-0 sm:text-sm sm:leading-6"
           :value="config.display()" />
  </div>
</template>

<style scoped>

</style>
