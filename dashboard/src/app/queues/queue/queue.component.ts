import {Component, effect, input, signal} from '@angular/core';
import {DecimalPipe, PercentPipe} from "@angular/common";

@Component({
    selector: 'app-queue',
    imports: [
        DecimalPipe,
        PercentPipe
    ],
    templateUrl: './queue.component.html',
    styleUrl: './queue.component.css',
})
export class QueueComponent {
    entry = input.required<number>()

    max = signal(1000);
    name = input.required<String>()

    constructor() {
        effect(() => {
            let max = this.max();
            let entry = this.entry();

            if (entry > max) {
                this.max.set(entry)
            }
        })

    }

    getQueueIcon(name: String): string {
        const lowerName = name.toLowerCase();

        if (lowerName.includes('definition')) return '🧬';
        if (lowerName.includes('neo') || lowerName.includes('postgres') || lowerName.includes('mongo')) return '🗃️';

        return '📦';
    }

}
