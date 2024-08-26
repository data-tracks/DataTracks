<script setup lang="ts">
import type { Stop } from '@/stores/plan'
import Button from '@/components/default/Button.vue'
import { useModalStore } from '@/stores/modal'
import Empty from '@/components/Empty.vue'


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

</script>

<template>
  <div class="flex gap-2 min-h-14 items-center">
    <div class="sources">
      <div v-for="source in stop?.sources" :key="source.id" class="p-4">
        <p>{{source._type}}</p>
      </div>
      <Button text="+ Source" @click="openAddSource()"></Button>
    </div>
    <div class="configuration grow border border-y-0 px-4 self-stretch flex items-center">
      <table class="table-fixed">
        <tbody>
        <tr>
          <td>Stop:</td>
          <td>{{ stop?.num }}</td>
        </tr>
        <template v-if="stop && stop.transform">
          <tr>
            <td>Language:</td>
            <td>{{ stop?.transform?.language }}</td>
          </tr>
          <tr>
            <td>Query:</td>
            <td>{{ stop?.transform?.query }}</td>
          </tr>
        </template>
        </tbody>
      </table>
    </div>
    <div class="destinations">
      <div v-for="destination in stop?.destinations" :key="destination.id" class="p-4">
        <p>{{destination._type}}</p>
      </div>
      <Button text="+ Destination" @click="openAddDestination()"></Button>
    </div>
  </div>
</template>

<style scoped>

</style>
