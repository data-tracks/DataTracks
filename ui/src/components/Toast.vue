<script setup lang="ts">
import { type Toast, ToastType, useToastStore } from '@/stores/toast'
import { storeToRefs } from 'pinia'
import IconClose from '@/components/icons/IconClose.vue'

const store = useToastStore()
const {toasts} = storeToRefs(store) //[{ message: "test", type: ToastType.success}];

const getClasses = (t: ToastType):string[] => {
  switch (t) {
    case ToastType.success:
      return ["bg-green-300"]
    case ToastType.error:
      return ["bg-red-300"]
    case ToastType.warning:
      return ["bg-yellow-300"]
  }
}


</script>

<template>
<div v-if="toasts && toasts.length > 0" class="z-10 py-4 absolute w-full">
  <div v-for="toast in toasts" :key="toast.message" class="m-4 rounded p-4 drop-shadow-lg flex justify-between" :class="getClasses(toast.type)">
    <div>
      {{toast.message}}
    </div>
    <div @click="store.removeToast(toast)">
     <IconClose  />
    </div>

  </div>
</div>
</template>

<style scoped>

</style>