import {Component, OnInit} from '@angular/core';
import {AsyncPipe} from "@angular/common";
import {EventsService} from "../events.service";
import {Observable, startWith} from "rxjs";


@Component({
    selector: 'app-status',
    imports: [
        AsyncPipe
    ],
    templateUrl: './status.html',
    styleUrl: './status.scss',
})
export class Status implements OnInit {

    public channels$: Observable<any[]> | undefined;

    constructor(private channelService: EventsService) {
    }

    ngOnInit() {
        this.channels$ = this.channelService.getUpdates().pipe(
            startWith([]) // Start with an empty list so the template doesn't crash
        );
    }


}
