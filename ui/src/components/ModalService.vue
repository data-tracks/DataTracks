<script setup lang="ts">
import { useModalStore } from '@/stores/modal'
import { storeToRefs } from 'pinia'
import Modal from '@/components/default/Modal.vue'

const store = useModalStore();

const {visible} = storeToRefs(store)

</script>

<template>
<Modal :visible="visible">
<h1>Test</h1>
</Modal>
</template>

<style scoped>

</style>