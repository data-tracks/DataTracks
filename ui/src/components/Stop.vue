<script setup lang="ts">
import type { Stop } from '@/stores/plan'
import Button from '@/components/default/Button.vue'
import { useModalStore } from '@/stores/modal'
import Empty from '@/components/Empty.vue'
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'


const props = defineProps<{
  stop: Stop | undefined
}>()

const modal = useModalStore();

const openAddSource = () => {
  modal.openModal(Empty, {text: "Add Source"});
}

const openAddDestination = () => {
  modal.openModal(Empty, {text: "Add Destination"});
}

const themeStore = useThemeStore();

const {isDark} = storeToRefs(themeStore)

</script>

<template>
  <div class="flex gap-2 min-h-14 items-center">
    <div class="sources  flex flex-col self-end items-center">
      <div v-for="source in stop?.sources" :key="source.id" class="p-4">
        <p>{{source._type}}</p>
      </div>
      <Button text="+ Source" @click="openAddSource()"></Button>
    </div>
    <div class="configuration grow border border-y-0 px-4 self-stretch flex items-center" >
      <table class="table-fixed">
        <tbody>
        <tr>
          <td>Stop:</td>
          <td>{{ stop?.num }}</td>
        </tr>
        <template v-if="stop?.transform?.configs">
          <tr :key="key" v-for="[key, config] in stop?.transform?.configs">
            <td>{{ key }}</td>
            <td>{{ config.display() }}</td>
          </tr>
        </template>
        <template v-else>
          <div class="flex rounded-md shadow-sm ring-1 ring-inset ring-gray-300 focus-within:ring-2 focus-within:ring-inset focus-within:ring-indigo-600 sm:max-w-md">
            <span class="flex select-none items-center pl-3 text-gray-500 sm:text-sm">workcation.com/</span>
            <input type="text" name="username" id="username" autocomplete="username" class="block flex-1 border-0 bg-transparent py-1.5 pl-1 text-gray-900 placeholder:text-gray-400 focus:ring-0 sm:text-sm sm:leading-6" placeholder="janesmith" />
          </div>
        </template>
        </tbody>
      </table>
    </div>
    <div class="destinations flex flex-col self-end items-center">
      <div v-for="destination in stop?.destinations" :key="destination.id" class="p-4">
        <p>{{destination._type}}</p>
      </div>
      <Button text="+ Destination" @click="openAddDestination()"></Button>
    </div>
  </div>
</template>

<style scoped>

</style>
