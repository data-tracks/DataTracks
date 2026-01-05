import {Injectable, NgZone, signal} from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class EventsService {

  private _channels = signal<any>(null);

  public channels = this._channels.asReadonly();

  constructor(private zone: NgZone) {
    this.initConnection()
  }

  private initConnection() {
    const socket = new WebSocket('ws://localhost:3131/events');

    socket.onmessage = (event) => {
      this.zone.run(() => {
        const data = JSON.parse(event.data);

        this._channels.set(data);
      });
    };

    socket.onclose = () => {
      console.warn('Disconnected from Rust. Retrying in 2s...');
      setTimeout(() => this.initConnection(), 2000);
    };
  }

}
