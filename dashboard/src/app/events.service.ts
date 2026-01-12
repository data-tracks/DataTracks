import {computed, Injectable, NgZone, signal} from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class EventsService {

  private _events = signal<any>(null);
  public events = this._events.asReadonly();

  private _queues = signal<any>(null);
  public queues = this._queues.asReadonly();

  public connectedStatistics = signal<boolean>(false);
  public connected = computed(() => this.connectedQueues() && this.connectedEvents() && this.connectedStatistics)

  public connectedEvents = signal<boolean>(false);
  public connectedQueues = signal<boolean>(false);
  private _statistics = signal<any>(null);
  public statistics = this._statistics.asReadonly();

  constructor(private zone: NgZone) {
    this.initEventsConnection();
    this.initQueueConnection();
    this.initStatisticsConnection();
  }

  private initEventsConnection() {
    const socket = new WebSocket('ws://localhost:3131/events');

    socket.onmessage = (event) => {
      this.zone.run(() => {
        this.connectedEvents.set(true);
        const data = JSON.parse(event.data);

        this._events.set(data);
      });
    };

    socket.onclose = () => {
      this.connectedEvents.set(false);
      console.warn('Disconnected from Rust. Retrying in 2s...');
      setTimeout(() => this.initEventsConnection(), 2000);
    };
  }

  private initQueueConnection() {
    const socket = new WebSocket('ws://localhost:3131/queues');

    socket.onmessage = (queue) => {
      this.zone.run(() => {
        this.connectedQueues.set(true);
        const data = JSON.parse(queue.data);

        this._queues.set(data);
      });
    };

    socket.onclose = () => {
      this.connectedQueues.set(false);
      console.warn('Disconnected from Rust. Retrying in 2s...');
      setTimeout(() => this.initQueueConnection(), 2000);
    };
  }

  private initStatisticsConnection() {
    const socket = new WebSocket('ws://localhost:3131/statistics');

    socket.onmessage = (queue) => {
      this.zone.run(() => {
        this.connectedStatistics.set(true);
        const data = JSON.parse(queue.data);

        this._queues.set(data);
      });
    };

    socket.onclose = () => {
      this.connectedStatistics.set(false);
      console.warn('Disconnected from Rust. Retrying in 2s...');
      setTimeout(() => this.initQueueConnection(), 2000);
    };
  }

}
