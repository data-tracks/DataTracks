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

export class ConfigContainer {
  name: string
  configs: Map<string, ConfigModel>

  static from(configContainer: ConfigContainer): ConfigContainer {
    const configs = new Map<string, ConfigModel>()
    console.log(configContainer.configs as Object)
    for (const [key, value] of Object.entries(configContainer.configs as Object)) {
      configs.set(key, ConfigModel.from(value))
    }
    return new ConfigContainer(configContainer.name, configs)
  }


  constructor(name: string, configs: Map<string, ConfigModel>) {
    this.name = name
    this.configs = configs
  }

  display(): string {
    return Array.from(this.configs).reduce((before, [key, value]) => {
      return before + `${capitalize(key)}:${value.display()}\n`
    }, '')
  }
}

function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1)
}

class BaseConfig {
  constructor(obj: Object = {}) {
  }

}

abstract class ConfigModel {
  baseConfig: BaseConfig

  protected constructor(baseConfig: BaseConfig) {
    this.baseConfig = baseConfig
  }

  static from(obj: any): ConfigModel {
    console.log(obj)
    if (Object.prototype.hasOwnProperty.call(obj, StringConf.name)) {
      return StringConf.from(obj[StringConf.name] as StringConf)
    } else if (Object.prototype.hasOwnProperty.call(obj, NumberConf.name)) {
      return NumberConf.from(obj[NumberConf.name] as NumberConf)
    } else if (Object.prototype.hasOwnProperty.call(obj, ListConf.name)) {
      return ListConf.from(obj[ListConf.name] as ListConf)
    } else {
      return new StringConf('Error', {})
    }
  }

  abstract display(): string;
}

class StringConf extends ConfigModel {
  string: string

  static from(object: StringConf): StringConf {
    return new StringConf(object.string, object.baseConfig)
  }

  constructor(string: string, baseConfig: BaseConfig) {
    super(baseConfig)
    this.string = string
  }

  display(): string {
    return this.string
  }
}

class NumberConf extends ConfigModel {
  number: number

  static from(object: NumberConf): NumberConf {
    return new NumberConf(object.number, object.baseConfig)
  }

  constructor(number: number, baseConfig: BaseConfig) {
    super(baseConfig)
    this.number = number
  }

  display(): string {
    return this.number.toString()
  }
}

class ListConf extends ConfigModel {
  list: ConfigModel[]
  addable: boolean


  constructor(object: ListConf) {
    super(object.baseConfig)
    this.list = object.list.map(e => ConfigModel.from(e))
    this.addable = object.addable
  }

  display(): string {
    return this.list.map(e => e.display()).join(', ')
  }


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
      const stop = data.stops[key] as Stop
      if (stop.transform) {
        stop.transform = ConfigContainer.from(stop.transform as ConfigContainer)
        stops.set(Number(key), stop)
      }
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
        configs: {
          'language': {
            StringConf: {
              string: 'sql'
            }
          },
          'query':
            {
              StringConf: {
                string: 'SELECT * FROM $1, $4, $5'
              }
            }
        }
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
