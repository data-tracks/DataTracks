import {Component, signal} from '@angular/core';
import {RouterOutlet} from '@angular/router';
import {StatusComponent} from "./status/status.component";

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterOutlet, StatusComponent],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('dashboard');
}
