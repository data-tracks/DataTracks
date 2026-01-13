import {Component, computed, effect, input, signal} from '@angular/core';
import {DecimalPipe} from "@angular/common";
import {QueueComponent} from "./queue/queue.component";

@Component({
  selector: 'app-queues',
  imports: [
    QueueComponent
  ],
  templateUrl: './queues.component.html',
  styleUrl: './queues.component.css',
})
export class QueuesComponent {
  inputs = input.required<any>();

  queues = signal(new Map<string, number>);

  queuesView = computed(() => Array.from(this.queues().entries()));

  sortedQueues = computed(() => {
    // We spread into a new array [...raw] because .sort() mutates the array
    return [...this.queuesView()].sort((a, b) => a[0].localeCompare(b[0]));
  }).bind(this);

  constructor() {
    effect(() => {
      let d = this.inputs();
      if (!d) {
        return
      }
      let data: Queue = d;


      this.queues.update(q => {
        let map = new Map(q);
        map.set(data.name, data.size);
        return map;
      })
    });
  }
}


export interface Queue {
  name: string;
  size: number
}
