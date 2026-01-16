import {Component, inject, signal} from '@angular/core';
import {EventsComponent} from "./events/events.component";
import {EventsService} from "./events.service";
import {QueuesComponent} from "./queues/queues.component";
import {StatisticsComponent} from "./statistics/statistics.component";

@Component({
  selector: 'app-root',
  imports: [EventsComponent, QueuesComponent, StatisticsComponent],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('dashboard');
  protected service = inject(EventsService);

  public activeTab = Tab.Stats;
  protected readonly Tab = Tab;
}

export enum Tab {
  Events,
  Queues,
  Stats
}