import { defineStore } from 'pinia'
import { type Component, type Ref, ref } from 'vue'
import Empty from '@/components/Empty.vue'

export const useModalStore = defineStore('modal', () => {
  const visible = ref(false)

  const content: Ref<Component> = ref(Empty)

  const properties = ref({})

  const openModal = (component: Component, props = {}) => {
    visible.value = true
    content.value = component
    properties.value = props
  }

  const closeModal = () => {
    visible.value = false
    content.value = Empty
    properties.value = {}
  }

  return { visible, content, properties, openModal, closeModal }
})
