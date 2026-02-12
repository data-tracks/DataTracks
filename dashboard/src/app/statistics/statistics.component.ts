import {Component, effect, input, signal, WritableSignal} from '@angular/core';
import {DecimalPipe, NgOptimizedImage} from "@angular/common";

@Component({
    selector: 'app-statistics',
    imports: [
        DecimalPipe,
        NgOptimizedImage
    ],
    templateUrl: './statistics.component.html',
    styleUrl: './statistics.component.css',
})
export class StatisticsComponent {
    inputs = input.required<any>();

    protected plainStatistics: WritableSignal<Map<string, [number, Stage, string, number][]>> = signal(new Map());
    protected mappedStatistics: WritableSignal<Map<string, [number, Stage, string, number][]>> = signal(new Map());

    protected tps: WritableSignal<Map<string, Throughput>> = signal(new Map())

    constructor() {
        effect(() => {
            let data = this.inputs();

            console.log(data)
            if (!data){
                return;
            }

            if (data.type == "Throughput"){
                let tps: ThroughputEvent = data.data;
                this.tps.update(map => {
                    let d = new Map(map);
                    for (let key in tps.tps) {
                        let value = tps.tps[key];
                        d.set(key, value)

                    }

                    return d
                })
                return;
            }
            let map: StatisticEvent = data.data;

            if (!map) {
                return;
            }

            this.plainStatistics.update(m => {
                let d = new Map(m);

                for (let key in map.engines) {
                    let entries = map.engines[key];

                    let values = [...entries[0]].filter((e) => e[1] == Stage.Plain).sort((a, b) => a[0] - b[0])
                    d.set(entries[1], values)
                }
                return d;
            });

            this.mappedStatistics.update(m => {
                let d = new Map(m);

                for (let key in map.engines) {
                    let entries = map.engines[key];

                    let values = [...entries[0]].filter((e) => e[1] == Stage.Mapped).sort((a, b) => a[0] - b[0])
                    d.set(entries[1], values)
                }
                return d;
            });

        });

    }

    protected getImage(engineName: string): string | null {
        let name = engineName.toLowerCase();
        if (name.includes("neo")) {
            return "/assets/neo.png"
        } else if (name.includes("mongo")) {
            return "/assets/mongo.png"
        } else if (name.includes("postgres")) {
            return "/assets/pg.png"
        }
        return null
    }

    protected getTp(name: string) {
        return this.tps().get(name) || {plain: 0, mapped: 0};
    }
}

export type DefinitionId = number;
export type EngineId = number;

export interface StatisticEvent {
    engines: Record<string, [[DefinitionId, Stage, string, number][], string]>;
}

export interface ThroughputEvent {
    tps: Record<string, Throughput>;
}

export enum Stage {
    Plain = "Plain",
    Mapped = "Mapped"
}

export interface Throughput {
    plain: number,
    mapped: number,
}