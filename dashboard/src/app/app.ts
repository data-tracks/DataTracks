import {Component, signal} from '@angular/core';
import {RouterOutlet} from '@angular/router';
import {Status} from "./status/status";

@Component({
    selector: 'app-root',
    imports: [RouterOutlet, Status],
    templateUrl: './app.html',
    styleUrl: './app.scss'
})
export class App {
    protected readonly title = signal('dashboard');
}
