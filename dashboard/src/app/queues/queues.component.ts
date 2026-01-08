import {Component, computed, effect, input, signal} from '@angular/core';
import {DecimalPipe} from "@angular/common";
import {QueueComponent} from "./queue/queue.component";

@Component({
  selector: 'app-queues',
  imports: [
    DecimalPipe,
    QueueComponent
  ],
  templateUrl: './queues.component.html',
  styleUrl: './queues.component.css',
})
export class QueuesComponent {
  inputs = input.required<any>();

  queues = signal(new Map<String, number>);

  queuesView = computed(() => Array.from(this.queues().entries()));

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
  name: String;
  size: number
}
