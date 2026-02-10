import {Component, computed, signal} from '@angular/core';
import {FormsModule} from "@angular/forms";
import {Value, ValueMapper} from "../value/util";

@Component({
  selector: 'app-testing',
  imports: [
    FormsModule
  ],
  templateUrl: './testing.component.html',
  styleUrl: './testing.component.css',
})
export class TestingComponent {
  protected messages = signal<Message[]>([])


  // Input State Signals
  topic = signal('');
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
    const bytes = ValueMapper.pack(valueObj);

    // Publish using your service...
    console.log(`Sending to ${this.topic()}:`, bytes);

    // Reset inputs
    this.textValue.set('');
    this.intValue.set(0);
    this.nodeLabels.set('');
  }
}

interface Message{
  topic: string,
  value: Value
}
