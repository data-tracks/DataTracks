import {Component, effect, inject, input, signal, WritableSignal} from '@angular/core';
import {EventsService} from "../events.service";

@Component({
    selector: 'app-status',
    templateUrl: './status.component.html',
    standalone: true,
    styleUrl: './status.component.css',
})
export class StatusComponent {

    protected msgs: WritableSignal<any[]> = signal([]);
    protected readonly JSON = JSON;

    inputs = input.required<any[]>();

    constructor() {
        effect(() => {
            let data = this.inputs();

            this.msgs.update(msgs => {
                const updated = [...msgs, data];
                return updated.slice(-10)
            })
        });

    }
}

