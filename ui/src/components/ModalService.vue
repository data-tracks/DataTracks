<script setup lang="ts">
import { useModalStore } from '@/stores/modal'
import { storeToRefs } from 'pinia'
import Modal from '@/components/default/Modal.vue'

const store = useModalStore();

const {visible, content} = storeToRefs(store)

</script>

<template>
<Modal :visible="visible">
  <component :is="content.value"></component>
</Modal>
</template>

<style scoped>

</style>
