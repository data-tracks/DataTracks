import {AfterViewInit, Component, effect, ElementRef, input, Input, OnChanges, signal, ViewChild} from '@angular/core';
import mermaid from 'mermaid';

@Component({
  selector: 'app-architecture',
  imports: [],
  templateUrl: './architecture.component.html',
  styleUrl: './architecture.component.css',
})
export class ArchitectureComponent implements AfterViewInit, OnChanges {
  @ViewChild('mermaidContainer') container!: ElementRef;
  inputs = input.required<any>();
  queues = new Map<string, number>;

  constructor() {
    effect(() => {
      let entry = this.inputs() as Entry;

      if (!entry) {
        return;
      }
      this.queues.set(entry.name, entry.size);
      this.renderGraph().catch(err => {
        console.log(err)
      })
    });

  }


  ngAfterViewInit() {
    mermaid.initialize({
      startOnLoad: false,
      theme: 'dark', // Use 'base' or 'dark' to match DaisyUI
      securityLevel: 'loose',
      flowchart: {
        curve: 'basis',                // Makes lines smoother
        htmlLabels: true,
      },
      // This is the "Spacing" setting
      themeVariables: {
        //edgeLabelBackground: 'transparent',
        tertiaryColor: '#ff0000' // Sometimes used for labels
      }
    });

    this.renderGraph().catch(err => {
      console.log(err)
    })
  }

  ngOnChanges() {
    this.renderGraph().catch(err => {
      console.log(err)
    })
  }

  async renderGraph() {
    if (!this.container) return;

    const timerVal = this.queues.get("Time Annotation -> WAL") || "0";
    const sinkVal = this.queues.get("Sink Input") || "0";
    const walVal = this.queues.get("WAL -> Engines") || "0";
    const pMongo = this.queues.get("Persister neo4j") || "0";
    const pPost = this.queues.get("Persister mongodb") || "0";
    const pNeo = this.queues.get("Persister postgres") || "0";

    const nNeo = this.queues.get("Definition Native 2 - Graph test to Engine") || "0";
    const nPost = this.queues.get("Definition Native 1 - Relational test to Engine") || "0";
    const nMongo = this.queues.get("Definition Native 0 - Document test to Engine") || "0";

    let graphDefinition = 'stateDiagram-v2\n' +
        `  direction LR\n` +
        `  [*] --> Sink\n` +
        `  Sink --> Timer ${this.formatNumber(sinkVal)}\n` +
        `  Timer --> WAL${this.formatNumber(timerVal)}\n` +
        `  WAL --> Persister${this.formatNumber(walVal)}\n` +
        `  state Persister{\n` +
        `  direction LR\n` +
        `  [*] --> PMongo${this.formatNumber(pMongo)}\n` +
        `  PMongo: Persister Mongo\n` +
        `  [*] --> PPost${this.formatNumber(pPost)}\n` +
        `  PPost: Persister Postgres\n` +
        `  [*] --> PNeo${this.formatNumber(pNeo)}\n` +
        `  PNeo: Persister Neo4j\n` +
        `  }\n` +
        `  Persister --> Nativer\n` +
        `  state Nativer{\n` +
        `  direction LR\n` +
        `  [*] --> NMongo${this.formatNumber(nMongo)}\n` +
        `  NMongo: Nativer Definition Mongo\n` +
        `  [*] --> NPost${this.formatNumber(nPost)}\n` +
        `  NPost: Nativer Definition Postgres\n` +
        `  [*] --> NNeo${this.formatNumber(nNeo)}\n` +
        `  NNeo: Nativer Definition Neo4j\n` +
        `  }\n`;

    graphDefinition += `  classDef healthy fill:#52c41a,color:#fff\n`;
    graphDefinition += `  classDef error fill:red,color:#fff\n`;

    const style = `
    <style>
      .edgeLabel { background: transparent !important; }
      .edgeLabel rect { padding: 2px; fill: rgba(0,0,0,0.1) !important; } /* Label background */
      .edgeLabels .edgeLabel{ 
        color: white !important; 
       }
      /*.edgeLabels .edgeLabel:nth-of-type(4) span{ 
        color: ${this.getQueueColor(walVal)} !important; 
      }
      .edgeLabels .edgeLabel:nth-of-type(3) span{ 
        color: ${this.getQueueColor(walVal)} !important; 
      }*/
    </style>
  `;

    const { svg } = await mermaid.render('mermaid-svg-' + Date.now(), graphDefinition);
    this.container.nativeElement.innerHTML = style + svg;
  }

  getQueueColor(value: string | number): string {
    const num = typeof value === 'string' ? parseInt(value) : value;
    if (isNaN(num)) return '#94a3b8'; // Gray for "loading..."
    if (num > 1000) return '#f87272'; // Red (DaisyUI Error)
    if (num > 500) return '#fbbd23';  // Orange (DaisyUI Warning)
    return '#36d399';                // Green (DaisyUI Success)
  }

  formatNumber(val: string | number): string {
    const num = typeof val === 'string' ? parseInt(val) : val;
    if (isNaN(num)) return "0";

    const number = num.toLocaleString('en-US').replace(/,/g, "'");

    return ":" + number;
  }
}

export interface Entry {
  name: string,
  size: number
}
