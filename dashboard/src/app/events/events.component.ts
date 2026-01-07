import {Component, effect, input, signal, WritableSignal} from '@angular/core';

@Component({
    selector: 'app-events',
    templateUrl: './events.component.html',
    standalone: true,
    styleUrl: './events.component.css',
})
export class EventsComponent {

    inputs = input.required<any[]>();
    protected msgs: WritableSignal<any[]> = signal([]);
    protected readonly JSON = JSON;

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

