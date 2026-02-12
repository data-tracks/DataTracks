import {Component, computed, effect, input, signal} from '@angular/core';
import {DatePipe, KeyValuePipe, NgClass} from "@angular/common";

@Component({
    selector: 'app-threads',
    imports: [
        DatePipe,
        KeyValuePipe,
        NgClass
    ],
    templateUrl: './threads.component.html',
    styleUrl: './threads.component.css',
})
export class ThreadsComponent {
    inputs = input.required<string>()

    threads = signal<Map<string, number>>(new Map());

    currentTime = signal(Date.now());
    groupedThreads = computed(() => {
        const threads = this.threads();
        const now = this.currentTime(); // Tracking dependency
        const STALE_THRESHOLD_MS = 5000; // Define what "stale" means (e.g., 2 seconds)

        const groups: Record<string, any[]> = {};

        for (const thread of threads) {
            const [id, timestamp] = thread;
            const groupName = id.split(' ')[0] || 'Unknown';

            if (!groups[groupName]) groups[groupName] = [];

            groups[groupName].push({
                id,
                timestamp,
                // Calculate stale flag here for maximum performance
                isStale: (now - timestamp) > STALE_THRESHOLD_MS,
                elapsed: now - timestamp
            });
        }

        // Sort: Stale items move to the bottom, active items stay at the top
        for (const key in groups) {
            groups[key].sort((a, b) => {
                if (a.isStale !== b.isStale) return a.isStale ? 1 : -1;
                return a.id.localeCompare(b.id);
            });
        }

        return groups;
    });

    constructor() {
        setInterval(() => this.currentTime.set(Date.now()), 100);

        effect(() => {
            let name = this.inputs();

            if (!name) {
                return;
            }

            const timestamp = Date.now()
            this.threads.update(t => {
                let map = new Map(t);
                map.set(name, timestamp);
                return map
            })
        });
    }

}
