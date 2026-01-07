import {Component, input} from '@angular/core';

@Component({
    selector: 'app-queue',
    imports: [],
    templateUrl: './queue.component.html',
    styleUrl: './queue.component.css',
})
export class QueueComponent {
    entry = input.required<number>()

}
