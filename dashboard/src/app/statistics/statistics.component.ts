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
    protected statistics: WritableSignal<Map<string, [number, string, number][]>> = signal(new Map());

    constructor() {
        effect(() => {
            let data = this.inputs();

            if (!data){
                return;
            }

            let map: StatisticEvent = data;

            console.log(map)

            this.statistics.update(m => {
                let d = new Map(m);

                for (let key in map.engines) {
                    let entries = map.engines[key];
                    let values = [...entries[0]].sort((a,b) => a[0] - b[0])
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
}

export type DefinitionId = number;
export type EngineId = number;

export interface StatisticEvent {
    engines: Record<string, [[DefinitionId, string, number][], string]>;
}
