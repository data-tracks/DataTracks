<script setup lang="ts">
import { useModalStore } from '@/stores/modal'
import { storeToRefs } from 'pinia'
import Modal from '@/components/default/Modal.vue'

const store = useModalStore()

const { visible, content, properties } = storeToRefs(store)

</script>

<template>
  <transition enter="ease-out duration-700" enter-from="opacity-0" enter-to="opacity-100" leave="ease-in duration-200"
              leave-from="opacity-100" leave-to="opacity-0">
    <Modal :visible="visible" class="p-4 top-0 left-0 absolute w-full h-full bg-gray-900 bg-opacity-70"
           @click="store.closeModal()">
      <component :is="content" v-bind="properties" @click.stop></component>
    </Modal>
  </transition>
</template>

<style scoped>

</style>
