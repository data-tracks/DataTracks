import {Component, signal} from '@angular/core';
import {form} from "@angular/forms/signals";
import {FormsModule} from "@angular/forms";

@Component({
  selector: 'app-testing',
  imports: [
    FormsModule
  ],
  templateUrl: './testing.component.html',
  styleUrl: './testing.component.css',
})
export class TestingComponent {

  topic = signal("");
  value = signal("");
  protected messages = signal<Message[]>([])

  protected send() {
    console.log(this.value())
    let msg =  {topic: this.topic(), value: this.value()};
    this.messages.update(ms => [...ms, msg])
    this.value.set("")
    this.topic.set("")
  }
}

interface Message{
  topic: string,
  value: string
}
