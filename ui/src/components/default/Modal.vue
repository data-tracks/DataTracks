<script setup lang="ts">
import { useModalStore } from '@/stores/modal'

defineProps<{
  visible: boolean
}>()

const store = useModalStore();
</script>

<template>
  <div v-if="visible" class=" z-10 w-full h-full grid content-center justify-center" @click="() => store.toggle()">
    <div class=" w-full h-full absolute bg-black opacity-10"></div>
    <div class="bg-blue-200 border-black border inline-block mx-auto rounded p-1 drop-shadow-lg">
      <slot />
    </div>
  </div>
</template>

<style scoped>

</style>