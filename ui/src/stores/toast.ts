import {type Ref, ref} from 'vue'
import {defineStore} from 'pinia'

export type Toast = {
    type: ToastType,
    message: string,
    seconds: number,
    handle?: ReturnType<typeof setTimeout>
}

export enum ToastType {
    success = 'success',
    error = 'error',
    warning = 'warning'
}


export const useToastStore = defineStore('toast', () => {
    const toasts: Ref<Toast[]> = ref([])

    const addToast = (message: string, type = ToastType.success, seconds = 10) => {
        const toast: Toast = {type, message, seconds}

        toast.handle = setInterval(() => {
            removeToast(toast)
        }, seconds * 1000)

        toasts.value.push(toast)
    }

    const removeToast = (toast: Toast) => {
        const index = toasts.value.indexOf(toast, 0)
        toasts.value.splice(index, 1)
        clearInterval(toast.handle)
    }

    return {toasts, addToast, removeToast}
})