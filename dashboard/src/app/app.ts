import {Component, inject, signal, WritableSignal} from '@angular/core';
import {RouterOutlet} from '@angular/router';
import {StatusComponent} from "./status/status.component";
import {EventsService} from "./events.service";
import {QueueComponent} from "./queue/queue.component";

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterOutlet, StatusComponent, QueueComponent],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('dashboard');
  protected service = inject(EventsService);
}
