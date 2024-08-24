<script setup lang="ts">


import type { Stop } from '@/stores/plan'
import Button from '@/components/default/Button.vue'

const props = defineProps<{
  stop: Stop | undefined
}>()
</script>

<template>
  <div>
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
    <div class="flex flex-col gap-2 items-center">
      <Button text="+ Attach"></Button>
    </div>
  </div>
</template>

<style scoped>

</style>
