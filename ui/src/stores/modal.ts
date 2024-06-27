import {defineStore} from 'pinia'
import {ref} from 'vue'

export const useModalStore = defineStore('modal', () => {
    const visible = ref(false)

    const toggle = () => {
        visible.value = !visible.value
    }

    return {visible, toggle}
})