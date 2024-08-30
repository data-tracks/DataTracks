import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import axios from 'axios'
import { ToastType, useToastStore } from '@/stores/toast'
import { IS_DUMMY_MODE, PORT } from '@/stores/plan'

export const useMetaStore = defineStore('meta', () => {
  const connectionStatus = ref(Status.Disconnected);
  const toast = useToastStore()

  const timout = setTimeout( async () => {
    await getStatus();
  }, 500);

  const getStatus = async () => {
    if (IS_DUMMY_MODE) {
      connectionStatus.value = Status.Debug;
      return
    }

    try {
      const {data, status } = await axios.get<GetStatus>(`http://localhost:${PORT}/status`)

      if (status !== 200 || !data.status) {
        connectionStatus.value = Status.Error;
        return
      }

      connectionStatus.value = data.status;
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
      console.log(error)
    }
  }

  return { connectionStatus }
})

export type GetStatus = {
  status: Status
}


export enum Status {
  Connected = "connected",
  Disconnected = "disconnected",
  Error = "error",
  Debug = "debug",
}
