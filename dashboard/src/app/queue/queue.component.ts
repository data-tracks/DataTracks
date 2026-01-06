import {Component, computed, effect, input, Input, signal, Signal} from '@angular/core';

@Component({
  selector: 'app-queue',
  imports: [],
  templateUrl: './queue.component.html',
  styleUrl: './queue.component.css',
})
export class QueueComponent {
  inputs = input.required<any>();

  queues = signal(new Map<String, number>);

  queuesView = computed(() => Array.from(this.queues().entries()));

  constructor() {
    effect(() => {
      let data: Queue = this.inputs();

      this.queues.update(q => {
        let map = new Map(q);
        map.set(data.name, data.size);
        return map;
      })
    });
  }

}


export interface Queue {
  name: String;
  size: number
}
