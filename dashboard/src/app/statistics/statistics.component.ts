import {Component, effect, input, signal, WritableSignal} from '@angular/core';

@Component({
    selector: 'app-statistics',
    imports: [],
    templateUrl: './statistics.component.html',
    styleUrl: './statistics.component.css',
})
export class StatisticsComponent {
    inputs = input.required<any>();
    protected msgs: WritableSignal<Map<string, WritableSignal<string>>> = signal(new Map());

    constructor() {
        effect(() => {
            let data = this.inputs();

            /*this.msgs.update(msgs => {
              return data
            })*/
            console.log(data)
        });

    }
}
