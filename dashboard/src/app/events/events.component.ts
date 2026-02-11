import {Component, effect, input, signal, WritableSignal} from '@angular/core';

@Component({
    selector: 'app-events',
    templateUrl: './events.component.html',
    standalone: true,
    styleUrl: './events.component.css',
})
class EventsComponent {

    inputs = input.required<Event>();
    protected msgs: WritableSignal<any[]> = signal([]);
    protected readonly JSON = JSON;

    protected filters = signal<string[]>([]);
    protected activeFilters = signal<string[]>([]);

    constructor() {
        effect(() => {
            let data = this.inputs();

            if (!this.filters().includes(data.type)){
                this.filters.update(filters => {
                    return [...filters, data.type];
                })

                this.activeFilters.update(filters => {
                    return [...filters, data.type];
                })
            }

            if(!this.activeFilters().includes(data.type)){
                return
            }

            this.msgs.update(msgs => {
                const updated = [...msgs, data];
                return updated.slice(-5)
            })
        });

    }

    protected toggleFilter(filter: string) {
        this.activeFilters.update(filters => {
            if(filters.includes(filter)) {
                return filters.filter(f => f !== filter)
            }else {
                return [...filters, filter]
            }
        })
    }
}

export default EventsComponent

export interface Event{
    type: string
}

