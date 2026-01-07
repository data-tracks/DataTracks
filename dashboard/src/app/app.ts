import {Component, inject, signal} from '@angular/core';
import {RouterOutlet} from '@angular/router';
import {EventsComponent} from "./events/events.component";
import {EventsService} from "./events.service";
import {QueuesComponent} from "./queues/queues.component";
import {StatusComponent} from "./status/status.component";

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterOutlet, EventsComponent, QueuesComponent, StatusComponent],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('dashboard');
  protected service = inject(EventsService);
}
