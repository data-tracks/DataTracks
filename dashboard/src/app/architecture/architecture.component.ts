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
      this.renderGraph();
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

    this.renderGraph();
  }

  ngOnChanges() {
    this.renderGraph();
  }

  async renderGraph() {
    if (!this.container) return;

    const timerVal = this.queues.get("Time Annotation -> WAL") || "0";
    const sinkVal = this.queues.get("Sink Input") || "0";
    const walVal = this.queues.get("WAL -> Engines") || "0";
    const pMongo = this.queues.get("Persister neo4j") || "0";
    const pPost = this.queues.get("Persister mongodb") || "0";
    const pNeo = this.queues.get("Persister postgres") || "0";

    let graphDefinition = 'graph LR\n' +
        `  Sink -- "${this.formatNumber(sinkVal)}" --> Timer\n` +
        `  Timer -- "${this.formatNumber(timerVal)}" --> WAL\n` +
        `  WAL -- "${this.formatNumber(walVal)}" --> Persister\n` +
        `  Persister -- "${this.formatNumber(pMongo)}" --> PMongo["Persister Mongo"]\n` +
        `  Persister -- "${this.formatNumber(pPost)}" --> PPostgres["Persister Postgres"]\n` +
        `  Persister -- "${this.formatNumber(pNeo)}" --> PNeo4j["Persister Neo4j"]\n` +
        `  Persister -- "702" --> Nativer\n`;

    graphDefinition += `  linkStyle 0 stroke:${this.getQueueColor(sinkVal)},color:${this.getQueueColor(sinkVal)},stroke-width:2px\n`;
    graphDefinition += `  linkStyle 1 stroke:${this.getQueueColor(timerVal)},color:${this.getQueueColor(timerVal)},stroke-width:2px\n`;
    graphDefinition += `  linkStyle 2 stroke:${this.getQueueColor(walVal)},color:${this.getQueueColor(walVal)},stroke-width:4px\n`;
    graphDefinition += `  linkStyle 3 stroke:${this.getQueueColor(pMongo)},color:${this.getQueueColor(pMongo)},stroke-width:4px\n`;
    graphDefinition += `  linkStyle 4 stroke:${this.getQueueColor(pPost)},color:${this.getQueueColor(pPost)},stroke-width:4px\n`;
    graphDefinition += `  linkStyle 5 stroke:${this.getQueueColor(pNeo)},color:${this.getQueueColor(pNeo)},stroke-width:4px\n`;

    const style = `
    <style>
      .edgeLabel { background: transparent !important; }
      .edgeLabel rect { padding: 2px; fill: rgba(0,0,0,0.1) !important; } /* Label background */
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

    return num.toLocaleString('en-US').replace(/,/g, "'");
  }
}

export interface Entry {
  name: string,
  size: number
}
