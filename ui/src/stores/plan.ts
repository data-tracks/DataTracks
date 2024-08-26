import { defineStore } from 'pinia'
import { type Ref, ref } from 'vue'
import axios from 'axios'
import { ToastType, useToastStore } from '@/stores/toast'

const PORT = import.meta.env.VITE_PORT || 8080
const IS_DUMMY_MODE = import.meta.env.VITE_MODE == 'dummy' || false

type Line = {
  num: number;
  stops: number[];
}

export type Stop = {
  num: number;
  transform: ConfigContainer;
  sources: Source[],
  destinations: Destination[],
}

type Source = {
  id: string,
  _type: string
}

type Destination = {
  id: string,
  _type: string
}

type ConfigContainer = {
  name: string,
  configs: Map<string, ConfigModel>
}

type Transform = {
  language: string;
  query: string;
}

type BaseConfig = {}

interface ConfigModel {
  baseConfig: BaseConfig
}

interface StringConf extends ConfigModel {
  string: string
}

interface NumberConf extends ConfigModel {
  number: number
}

interface ListConf extends ConfigModel {
  list: ConfigModel[],
  addable: boolean
}

export type Network = {
  name: string;
  lines: Map<number, Line>;
  stops: Map<number, Stop>;
}

export const getStop = (network: Network, number: number): Stop | undefined => {
  return network.stops.get(number)
}

export type Node = {
  num: number
  x: number
  y: number
}

export type Link = {
  source: Node
  target: Node
}

type GetPlansResponse = {
  plans: any[]
}


export const usePlanStore = defineStore('plan', () => {
  const plans: Ref<Array<Network>> = ref([])
  const toast = useToastStore()
  const currentNumber = ref<number | null>()

  const setCurrent = (number: number | null) => {
    currentNumber.value = number
  }

  const transformNetwork = (data: any): Network => {
    const lines = new Map<number, Line>()
    const stops = new Map<number, Stop>()

    for (const key in data.lines) {
      lines.set(Number(key), data.lines[key] as Line)
    }

    for (const key in data.stops) {
      stops.set(Number(key), data.stops[key] as Stop)
    }

    return {
      name: data.name,
      lines: lines,
      stops: stops
    }
  }

  const submitPlan = async (name: string, plan: string) => {
    try {
      await axios.post('http://localhost:' + PORT + '/plans/create', { name: name, plan: plan })
      toast.addToast('Successfully created plan: ' + name + '.')
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
    }
  }

  const fetchPlans = async () => {
    if (IS_DUMMY_MODE) {
      plans.value = _dummyData.map(d => transformNetwork(d))
      return
    }

    try {
      const { data, status } = await axios.get<GetPlansResponse>('http://localhost:' + PORT + '/plans')

      if (status !== 200 || !data.plans) {
        return
      }

      plans.value = data.plans.map(d => transformNetwork(d))
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
      console.log(error)
    }
  }

  return { plans, currentNumber, setCurrent, submitPlan, fetchPlans }
})

const _dummyData: any[] = [{
  name: 'Plan Simple',
  lines: {
    0: {
      num: 0,
      stops: [0, 1, 3]
    },
    1: {
      num: 1,
      stops: [4, 1]
    },
    2: {
      num: 2,
      stops: [5, 1]
    },
    3: {
      num: 3,
      stops: [6, 7]
    }
  },
  stops: {
    0: {
      num: 0,
      sources: [
        {
          _type: 'mongo',
          id: 'test_mongo'
        }
      ]
    },
    1: {
      num: 1,
      transform: {
        name: 'Transform',
        configs: [
          [
            'language',
            {
              StringConf: {
                string: 'sql'
              }
            }
          ],
          [
            'query',
            {
              StringConf: {
                string: 'SELECT * FROM $1, $4, $5'
              }
            }
          ]
        ]
      }
    },
    3: {
      num: 3
    },
    4: {
      num: 4
    },
    5: {
      num: 5
    },
    6: {
      num: 6
    },
    7: {
      num: 7,
      destinations: [
        {
          _type: 'mqtt',
          id: 'test_mqtt'
        }
      ]
    }
  }
}]
