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
      <Button text="+ Source" @click="openAddSource()"></Button>
    </div>
    <div class="configuration grow border border-y-0 px-4">
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
      <Button text="+ Destination" @click="openAddDestination()"></Button>
    </div>
  </div>
</template>

<style scoped>

</style>
