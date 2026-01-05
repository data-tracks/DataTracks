import {Injectable, NgZone} from '@angular/core';
import {Observable} from "rxjs";

@Injectable({
    providedIn: 'root',
})
export class EventsService {
    constructor(private zone: NgZone) {
    }

    getUpdates(): Observable<String[]> {
        return new Observable(observer => {
            const socket = new WebSocket('ws://localhost:3131/events');

            socket.onmessage = (event) => {
                console.log(event.data)
                this.zone.run(() => observer.next(JSON.parse(event.data).toString()));
            };

            socket.onerror = (err) => {
                this.zone.run(() => observer.error(err));
            };

            return () => socket.close();
        });
    }

}
