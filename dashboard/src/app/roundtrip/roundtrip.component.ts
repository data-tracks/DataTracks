import {Component, computed, inject, NgZone, signal} from '@angular/core';
import {FormsModule} from "@angular/forms";
import {Value, ValueMapper} from "../value/util";

@Component({
  selector: 'app-roundtrip',
  imports: [
    FormsModule
  ],
  templateUrl: './roundtrip.component.html',
  styleUrl: './roundtrip.component.css',
})
export class RoundtripComponent {
  protected messages = signal<Message[]>([])

  protected connected = signal(false);

  protected listening = signal(false);

  protected _received = signal<Message[]>([]);

  protected socket = signal<WebSocket | null>(null);

  zone = inject(NgZone)

  // Input State Signals
  sendTopic = signal('');
  recTopic = signal('');

  selectedType = signal<string>('Text');

  // Value-specific Signals
  textValue = signal('');
  intValue = signal<number>(0);
  boolValue = signal<boolean>(false);
  nodeId = signal<number>(0);
  nodeLabels = signal('');

  // Computed: Real-time preview of the Value object before packing
  currentValue = computed<Value>(() => {
    switch (this.selectedType()) {
      case 'Int': return { type: 'Int', value: BigInt(this.intValue()) };
      case 'Bool': return { type: 'Bool', value: this.boolValue() };
      case 'Node': return {
        type: 'Node',
        id: BigInt(this.nodeId()),
        labels: this.nodeLabels().split(',').map(s => s.trim()).filter(s => !!s),
        properties: {}
      };
      default: return { type: 'Text', value: this.textValue() };
    }
  });

  send() {
    const valueObj = this.currentValue();
    const bytes = ValueMapper.pack([valueObj]);

    // Publish using your service...
    console.log(`Sending to ${this.sendTopic()}:`, bytes);

    // Reset inputs
    this.textValue.set('');
    this.intValue.set(0);
    this.nodeLabels.set('');
  }

  protected listen() {
    this.initConnection(this.recTopic())
    this.listening.set(true);
  }

  protected readonly console = console;

  protected stopListen() {
    this.socket.update(s => {
      if (s != null) {
        s.close(0, "Force close");
      }
      return null;
    })
    this.listening.set(false);
  }

  private initConnection(topic: string) {
    let socket = new WebSocket(`ws://localhost:3131/channel/${topic}`);
    socket.binaryType = "arraybuffer";
    this.socket.set(socket);

    socket.onmessage = (event) => {
      this.zone.run(() => {
        this.connected.set(true);
        const values = ValueMapper.unpack(event.data);

        console.log(event.data)

        this._received.update(mgs => [...mgs, {topic: topic, values: values }]);
      });
    };

    socket.onclose = () => {
      this.connected.set(false);
      console.warn('Disconnected from Rust. Retrying in 2s...');
      setTimeout(() => this.initConnection(topic), 2000);
    };
  }
}

interface Message{
  topic: string,
  values: Value[]
}
