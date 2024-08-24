import {defineStore} from 'pinia'
import {ref} from 'vue'
import Empty from '@/components/Empty.vue'

export const useModalStore = defineStore('modal', () => {
    const visible = ref(false)

    const content = ref(Empty);

    const toggle = () => {
        visible.value = !visible.value
    }

    return {visible, content, toggle}
})
