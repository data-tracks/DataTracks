import {Component, effect, Input, input, signal} from '@angular/core';
import {DecimalPipe} from "@angular/common";

@Component({
    selector: 'app-queue',
    imports: [
        DecimalPipe
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

}
