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
        s.close(1000, "User requested disconnect");
      }
      this.recTopic.set("")
      return null;
    })
    this.connected.set(false);
  }

  private initConnection(topic: string) {
    this._received.set([])
    let socket = new WebSocket(`ws://localhost:3131/channel/${topic}`);
    socket.binaryType = "arraybuffer";
    this.socket.set(socket);

    (BigInt.prototype as any).toJSON = function () {
      return this.toString();
    };
    this.connected.set(true);
    socket.onmessage = (event) => {
      this.zone.run(() => {
        const uint8Array = new Uint8Array(event.data);
        const values = ValueMapper.unpack(uint8Array);

        this._received.update(mgs => {
          let init = mgs;
          if(init.length >= 50){
            console.log("in")
            init = mgs.slice(1, 50)
          }

          let updated = [...init, {topic: topic, values: values }];
          return updated.slice(0, 50);
        });
      });
    };

    socket.onclose = () => {
      if (!this.connected()) {
        return
      }
      console.warn('Disconnected from Rust. Retrying in 2s...');
      this.connected.set(false);
      setTimeout(() => this.initConnection(topic), 2000);
    };
  }

  protected readonly JSON = JSON;
}

interface Message{
  topic: string,
  values: Value[]
}
