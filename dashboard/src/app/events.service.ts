import {Injectable, NgZone, signal} from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class EventsService {

  private _events = signal<any>(null);
  public events = this._events.asReadonly();

  private _queues = signal<any>(null);
  public queues = this._queues.asReadonly();

  constructor(private zone: NgZone) {
    this.initEventsConnection();
    this.initQueueConnection();
  }

  private initEventsConnection() {
    const socket = new WebSocket('ws://localhost:3131/events');

    socket.onmessage = (event) => {
      this.zone.run(() => {
        const data = JSON.parse(event.data);

        this._events.set(data);
      });
    };

    socket.onclose = () => {
      console.warn('Disconnected from Rust. Retrying in 2s...');
      setTimeout(() => this.initEventsConnection(), 2000);
    };
  }

  private initQueueConnection() {
    const socket = new WebSocket('ws://localhost:3131/queues');

    socket.onmessage = (queue) => {
      this.zone.run(() => {
        const data = JSON.parse(queue.data);

        this._queues.set(data);
      });
    };

    socket.onclose = () => {
      console.warn('Disconnected from Rust. Retrying in 2s...');
      setTimeout(() => this.initQueueConnection(), 2000);
    };
  }

}
