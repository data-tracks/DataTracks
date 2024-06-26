import {defineStore} from 'pinia'
import {type Ref, ref} from 'vue'
import axios from 'axios'
import {ToastType, useToastStore} from '@/stores/toast'

type Line = {
    num: number;
    stops: number[];
}

type Stop = {
    num: number;
    transform: Transform;
    inputs?: number[],
    outputs?: number[],
}

type Transform = {
    language: string;
    query: string;
}

export type Network = {
    name: string;
    lines: Map<number, Line>;
    stops: Map<number, Stop>;
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

    function transformNetwork(data: any): Network {
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

    async function submitPlan(name: string, plan: string) {
        try {
            await axios.post('http://localhost:2666' + '/plans/create', {name: name, plan: plan})
            toast.addToast('Successfully created plan: ' + name + '.')
        } catch (error) {
            toast.addToast(error as string, ToastType.error)
        }
    }

    async function fetchPlans() {
        try {
            const {data, status} = await axios.get<GetPlansResponse>('http://localhost:2666' + '/plans')

            if (status !== 200 || !data.plans) {
                return
            }

            plans.value = data.plans.map(d => transformNetwork(d))
        } catch (error) {
            toast.addToast(error as string, ToastType.error)
            console.log(error)
        }
    }

    return {plans, submitPlan, fetchPlans}
})

const dummyData: any[] = [{
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
            num: 0
        },
        1: {
            num: 1,
            transform: {
                language: 'SQL',
                query: 'SELECT * FROM $1, $4, $5'
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
            num: 7
        }
    }
}]