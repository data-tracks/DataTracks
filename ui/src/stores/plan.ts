import {defineStore} from 'pinia'
import {type Ref, ref} from 'vue'
import axios from 'axios'
import {ToastType, useToastStore} from '@/stores/toast'
import Stop from '@/components/Stop.vue'

export const PORT = import.meta.env.VITE_PORT || 8080
export const IS_DUMMY_MODE = import.meta.env.VITE_MODE == 'dummy' || false

type Line = {
  num: number;
  stops: number[];
}

export type Stop = {
  id: number;
  num: number;
  transform: ConfigContainer;
  sources: Source[],
  destinations: Destination[],
}

export type Source = {
  id: string,
  type_name: string,
  configs: ConfigModel[],
}

export type Destination = {
  id: string,
  type_name: string,
  configs: ConfigModel[],
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
      return before + `<div>${capitalize(key)}: ${value.display()}\n</div>`
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

export abstract class ConfigModel {
  baseConfig: BaseConfig

  protected constructor(baseConfig: BaseConfig) {
    this.baseConfig = baseConfig
  }

  static from(obj: any): ConfigModel {
    console.log(obj)
    if (Object.prototype.hasOwnProperty.call(obj, StringConf.key)) {
      return StringConf.from(obj[StringConf.key] as StringConf)
    } else if (Object.prototype.hasOwnProperty.call(obj, NumberConf.key)) {
      return NumberConf.from(obj[NumberConf.key] as NumberConf)
    } else if (Object.prototype.hasOwnProperty.call(obj, ListConf.key)) {
      return ListConf.from(obj[ListConf.key] as ListConf)
    } else {
      return new StringConf('Error', {})
    }
  }

  abstract display(): string;
}

class StringConf extends ConfigModel {
  string: string
  static key = 'StringConf'

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
  static key = 'NumberConf'

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
  static key = 'ListConf'
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

export type Plan = {
  id: number;
  status: PlanStatus;
  name: string;
  lines: Map<number, Line>;
  stops: Map<number, Stop>;
}

export enum PlanStatus {
  Running = 'Running',
  Stopped = 'Stopped',
  Error = 'Error'
}

export const getStop = (plan: Plan, number: number): Stop | undefined => {
  return plan.stops.get(number);
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
  const plans: Ref<Array<Plan>> = ref([])
  const toast = useToastStore()
  const currentNumbers = ref(new Map<number, number | null>())

  const setCurrent = (planId: number, stop: number | null) => {
    currentNumbers.value.set(planId, stop)
  }

  const transformPlan = (data: any): Plan => {
    const lines = new Map<number, Line>()
    const stops = new Map<number, Stop>()

    for (const key in data.lines) {
      lines.set(Number(key), data.lines[key] as Line)
    }

    for (const key in data.stops) {
      const stop = data.stops[key] as Stop
      if (stop.transform) {
        stop.transform = ConfigContainer.from(stop.transform as ConfigContainer)
      }
      stops.set(Number(key), stop)
    }

    return {
      status: data.status || PlanStatus.Stopped,
      id: data.id,
      name: data.name,
      lines: lines,
      stops: stops
    }
  }

  const submitPlan = async (name: string, plan: string) => {
    try {
      await axios.post(`http://localhost:${PORT}/plans/create`, {name: name, plan: plan})
      toast.addToast(`Successfully created plan: ${name}.`)
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
    }
  }

  const startPlan = async (planId: number) => {
    try {
      await axios.post(`http://localhost:${PORT}/plans/start`, {plan_id: planId})
      toast.addToast(`Successfully started plan: ${planId}.`)
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
    }
  }

  const stopPlan = async (planId: number) => {
    try {
      await axios.post(`http://localhost:${PORT}/plans/stop`, {plan_id: planId})
      toast.addToast(`Successfully stopped plan: ${planId}.`)
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
    }
  }

  const fetchPlans = async () => {
    if (IS_DUMMY_MODE) {
      plans.value = _dummyData.map(d => transformPlan(d))
      return
    }

    try {
      const {data, status} = await axios.get<GetPlansResponse>(`http://localhost:${PORT}/plans`)

      if (status !== 200 || !data.plans) {
        return
      }

      plans.value = data.plans.map(d => transformPlan(d))
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
      console.log(error)
    }
  }

  const addInOut = async (planId: number, stopId: number, typeName: string, category: InOut, configs: ConfigModel[]) => {
    if (IS_DUMMY_MODE) {
      toast.addToast(`Cannot add ${InOut} in dummy mode.`, ToastType.error)
      return
    }
    try {
      await axios.post(`http://localhost:${PORT}/inouts/create`, {
        plan_id: planId,
        stop_id: stopId,
        type_name: typeName,
        category,
        configs
      })
      toast.addToast(`Successfully added :${typeName}.`)
      await fetchPlans();
    } catch (error) {
      toast.addToast(error as string, ToastType.error)
    }
  }

  return {plans, currentNumbers, setCurrent, submitPlan, fetchPlans, addInOut, startPlan, stopPlan}
})

const _dummyData: any[] = [{
  name: 'Plan Simple',
  status: "stopped",
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
          type_name: 'mongo',
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
          type_name: 'mqtt',
          id: 'test_mqtt'
        }
      ]
    }
  }
}]

export enum InOut {
  Source = 'source',
  Destination = 'destination'
}
