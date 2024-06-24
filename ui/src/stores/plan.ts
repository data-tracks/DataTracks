import {defineStore} from 'pinia'
import {type Ref, ref} from 'vue'

interface Line {
    num: number;
    stops: number[];
}

interface Stop {
    num: number;
    transform: Transform;
}

interface Transform {
    language: string;
    query: string;
}

export interface Network {
    name: string;
    lines: Map<number, Line>;
    stops: Map<number, Stop>;
}


export const usePlanStore = defineStore('plan', () => {
    const plans: Ref<Array<Network>> = ref([])

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

    async function fetchPlans() {
        try {
            //const data = await axios.get('/plans/get')
            const data = [{
                name: "Plan Simple",
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
            plans.value = [transformNetwork(data)]
        } catch (error) {
            alert(error)
            console.log(error)
        }
    }

    return {plans, fetchPlans}
})