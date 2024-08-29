import {type Ref, ref} from 'vue'
import {defineStore} from 'pinia'
import {ConfigContainer, type ConfigModel, type Destination, IS_DUMMY_MODE, PORT, type Source} from '@/stores/plan'
import axios from 'axios'
import {ToastType, useToastStore} from '@/stores/toast'

export type SourceDestinationModel = {
  type_name: string,
  type: 'source'|'destination',
  configs: ConfigContainer
}

export class Addable{
  name:string;
    configs: ConfigModel[];
    onAdd: (name: string, configs: ConfigModel[]) => void;


    constructor(name: string, configs: ConfigModel[], onAdd: (name: string, configs: ConfigModel[]) => void) {
    this.name = name
        this.configs = configs
        this.onAdd = onAdd
  }
}


export const useOptionsStore = defineStore('options', () => {
  const sourcesDestinations: Ref<SourceDestinationModel[]> = ref([])

  const toast = useToastStore();

  const sources:Ref<Source[]> = ref([]);

  const destinations:Ref<Destination[]> = ref([]);

  type GetOptions = {
    sources: Source[],
    destinations: Destination[],
  }


  const fetchInOutOptions = async () => {
    if (IS_DUMMY_MODE) {
      sourcesDestinations.value =  _dummyData as SourceDestinationModel[];
      return
    }

    try {
      const { data, status } = await axios.get<GetOptions>('http://localhost:' + PORT + '/options')

      if (status !== 200 || !data.destinations || !data.sources) {
        return
      }

      sources.value = data.sources as Source[];
      destinations.value = data.destinations as Destination[];
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
      console.log(error)
    }
  }

  return {sources, destinations, fetchInOutOptions}
})

const _dummyData = {}
