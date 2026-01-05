import {Component, effect, inject, signal, WritableSignal} from '@angular/core';
import {EventsService} from "../events.service";

@Component({
    selector: 'app-status',
    templateUrl: './status.component.html',
    standalone: true,
    styleUrl: './status.component.css',
})
export class StatusComponent {

    protected service = inject(EventsService);

    protected msgs: WritableSignal<any[]> = signal([]);
    protected readonly JSON = JSON;

    constructor() {
        effect(() => {
            let data = this.service.channels();

            this.msgs.update(msgs => {
                const updated = [...msgs, data];
                return updated.slice(-10)
            })
        });

    }
}

