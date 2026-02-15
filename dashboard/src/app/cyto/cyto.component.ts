import { AfterViewInit, Component, effect, ElementRef, input, ViewChild, OnDestroy } from '@angular/core';
import cytoscape from 'cytoscape';

@Component({
  selector: 'app-cyto',
  standalone: true,
  templateUrl: `./cyto.component.html`,
})
export class CytoComponent implements AfterViewInit {
  @ViewChild('cyContainer') container!: ElementRef;
  inputs = input.required<any>();

  private cy?: cytoscape.Core;
  private queues = new Map<string, number>();

  constructor() {
    effect(() => {
      const entry = this.inputs();
      if (!entry) return;

      this.queues.set(entry.name, entry.size);
      this.updateNodeValues(); // Update existing graph instead of re-rendering
    });
  }

  ngAfterViewInit() {
    this.initGraph();
  }

  private initGraph() {
    this.cy = cytoscape({
      container: this.container.nativeElement,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': '#2a323c',
            'label': 'data(label)',
            'color': '#fff',
            'text-valign': 'center',
            'text-halign': 'center',
            'width': '150px',
            'shape': 'round-rectangle',
            'font-size': '12px'
          }
        },
        {
          selector: 'edge',
          style: {
            'width': 2,
            'line-color': '#444',
            'target-arrow-color': '#444',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            'label': 'data(value)', // This maps to the numeric value
            'color': 'data(color)',
            'font-weight': 'bold',
            'text-background-opacity': 0.8,
            'text-background-color': '#1a1a1a',
            'text-background-padding': '2px'
          }
        },
        {
          selector: ':parent', // Compounds (Persister/Nativer)
          style: {
            'background-opacity': 0.1,
            'border-color': '#555',
            'label': 'data(id)'
          }
        }
      ],
      elements: this.getInitialElements(),
      layout: {
        name: 'grid', // You can use 'dagre' via extension for LR flow
        rows: 7,
        cols: 3
      }
    });
  }

  private updateNodeValues() {
    if (!this.cy) return;

    // Mapping your Map keys to Cytoscape element IDs
    const mapping: { [key: string]: string } = {
      "Sink Input": "edge-sink-timer",
      "Time Annotation -> WAL": "edge-timer-wal",
      "WAL -> Engines": "edge-wal-persister",
      "Persister mongodb": "node-p-mongo",
      "Persister postgres": "node-p-post",
      "Persister neo4j": "node-p-neo"
    };

    this.queues.forEach((value, key) => {
      const id = mapping[key];
      if (!id) return;

      const element = this.cy?.getElementById(id);
      if (element) {
        // Update data without refreshing layout
        element.data('value', this.formatNumber(value));
        element.data('color', this.getQueueColor(value));
      }
    });
  }

  private getInitialElements(): cytoscape.ElementDefinition[] {
    return [
      // Nodes
      { data: { id: 'sink', label: 'Sink' } },
      { data: { id: 'timer', label: 'Timer' } },
      { data: { id: 'wal', label: 'WAL' } },
      { data: { id: 'persister', label: 'Persister' } },
      //{ data: { id: 'node-p-mongo', parent: 'Persister', label: 'Mongo' } },
      //{ data: { id: 'node-p-post', parent: 'Persister', label: 'Postgres' } },
      //{ data: { id: 'node-p-neo', parent: 'Persister', label: 'Neo4j' } },
      { data: { id: 'nativer1', label: 'Nativer 1' } },
      { data: { id: 'nativer2', label: 'Nativer 2' } },
      { data: { id: 'nativer3', label: 'Nativer 3' } },

      // Edges with initial values
      { data: { id: 'edge-sink-timer', source: 'sink', target: 'timer', value: "0", color: '#fff' } },
      { data: { id: 'edge-timer-wal', source: 'timer', target: 'wal', value: '0', color: '#fff' } },
      { data: { id: 'edge-wal-persister', source: 'wal', target: 'persister', value: '0', color: '#fff' } },
      { data: { id: 'edge-persister-nativer1', source: 'persister', target: 'nativer1', value: '0', color: '#fff' } },
      { data: { id: 'edge-persister-nativer2', source: 'persister', target: 'nativer2', value: '0', color: '#fff' } },
      { data: { id: 'edge-persister-nativer3', source: 'persister', target: 'nativer3', value: '0', color: '#fff' } },
    ];
  }

  getQueueColor(num: number): string {
    if (num > 1000) return '#f87272';
    if (num > 500) return '#fbbd23';
    return '#36d399';
  }

  formatNumber(num: number): string {
    return num.toLocaleString('en-US').replace(/,/g, "'");
  }
}
