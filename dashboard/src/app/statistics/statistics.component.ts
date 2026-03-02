import {Component, input, OnInit, signal, WritableSignal} from '@angular/core';
import {DecimalPipe, NgOptimizedImage} from "@angular/common";
import {Observable, Subject} from "rxjs";

@Component({
    selector: 'app-statistics',
    imports: [
        DecimalPipe,
        NgOptimizedImage
    ],
    templateUrl: './statistics.component.html',
    styleUrl: './statistics.component.css',
})
export class StatisticsComponent implements OnInit {
    inputs = input.required<Observable<any>>();
    private queue$ = new Subject<any>();

    protected delay: WritableSignal<string | undefined> = signal(undefined);

    protected plainStatistics: WritableSignal<Map<string, [number, Stage, string, number][]>> = signal(new Map());
    protected mappedStatistics: WritableSignal<Map<string, [number, Stage, string, number][]>> = signal(new Map());

    protected tps: WritableSignal<Map<string, Throughput>> = signal(new Map())

    ngOnInit() {
        // Subscribe to the stream passed from the parent
        this.inputs().subscribe(data => {
            this.processData(data);
        });
    }


    private processData(data: any) {
        if (data.type === "Throughput") {
            const tps: ThroughputEvent = data.data;
            this.tps.update(map => {
                const d = new Map(map);
                for (const key in tps.tps) {
                    d.set(key, tps.tps[key]);
                }
                return d;
            });
            return;
        }

        const stats: StatisticEvent = data.data;
        if (!stats) return;

        // Process Statistics
        this.updateStats(stats);
    }

    private updateStats(map: StatisticEvent) {
        this.delay.set(this.formatDuration(map.delay));

        this.plainStatistics.update(m => {
            const d = new Map(m);
            for (const key in map.engines) {
                const [entries, engineName] = map.engines[key];
                const values = [...entries].filter(e => e[1] === Stage.Plain).sort((a, b) => a[0] - b[0]);
                d.set(engineName, values);
            }
            return d;
        });

        this.mappedStatistics.update(m => {
            const d = new Map(m);
            for (const key in map.engines) {
                const [entries, engineName] = map.engines[key];
                const values = [...entries].filter(e => e[1] === Stage.Mapped).sort((a, b) => a[0] - b[0]);
                d.set(engineName, values);
            }
            return d;
        });
    }

    private formatDuration(ms: number): string {
        if (ms < 1000) return `${ms}ms`;

        const seconds = Math.floor((ms / 1000) % 60);
        const minutes = Math.floor((ms / 60000) % 60);
        const hours = Math.floor(ms / 3600000);
        ms = Math.floor(ms % 1000);

        const parts = [];
        if (hours > 0) parts.push(`${hours}h`);
        if (minutes > 0) parts.push(`${minutes}m`);
        if (seconds > 0) parts.push(`${seconds}s`);
        if (ms > 0 || parts.length === 0) parts.push(`${ms}ms`);

        return parts.join(' ');
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
    delay: number
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