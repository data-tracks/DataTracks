import {AfterViewInit, Component, effect, ElementRef, input, ViewChild} from '@angular/core';
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
            'width': '75px',
            'shape': 'round-rectangle',
            'font-size': '12px'
          }
        },
        {
          selector: 'edge',
          style: {
            'width': 2,
            'line-color': 'data(color)',
            'target-arrow-color': 'data(color)',
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
        },
        {
          selector: 'node.rotate',
          style: {
            'text-rotation': 1.6,
            'width': '20px',
            'height': '80px'
          }
        },
        {
          selector: 'edge.rotate',
          style: {
            'target-endpoint': '0% -50%',  // End at the left-middle of the target node
            'text-rotation': 1.6,
          }
        },
        {
          selector: 'edge.no-label', // Targets edges with the 'no-label' class
          style: {
            'line-color': 'grey',
            'target-arrow-color': 'gray',
            'label': '', // This removes the text/number
            'text-background-opacity': 0 // Optional: ensures no ghost box remains
          }
        }
      ],
      elements: this.getInitialElements(),
      layout: {
        name: 'preset'
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
      "Persister mongodb": "edge-persister1",
      "Persister postgres": "edge-persister2",
      "Persister neo4j": "edge-persister3",
      "Definition-0-Document test": "edge-nativer0",
      "Definition-1-Relational test": "edge-nativer1",
      "Definition-2-Graph test": "edge-nativer2"
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
      {data: {id: 'sink', label: 'Sink'}, position: {x: 0, y: 100}},
      {data: {id: 'timer', label: 'Timer'}, position: {x: 150, y: 100}},
      {data: {id: 'wal', label: 'WAL'}, position: {x: 300, y: 100}},

      // Persisters
      {data: {id: 'persister', label: 'Persister'}, position: {x: 450, y: 100}},
      {data: {id: 'persister1', label: 'Mongo'}, position: {x: 400, y: 300}, classes: "rotate"},
      {data: {id: 'persister2', label: 'Postgres'}, position: {x: 450, y: 300}, classes: "rotate"},
      {data: {id: 'persister3', label: 'Neo4j'}, position: {x: 500, y: 300}, classes: "rotate"},

      // Nativers
      {data: {id: 'nativer', label: 'Nativer'}, position: {x: 600, y: 100}},
      {data: {id: 'nativer0', label: 'Definition 0'}, position: {x: 550, y: 300}, classes: "rotate"},
      {data: {id: 'nativer1', label: 'Definition 1'}, position: {x: 600, y: 300}, classes: "rotate"},
      {data: {id: 'nativer2', label: 'Definition 2'}, position: {x: 650, y: 300}, classes: "rotate"},

      // end
      {data: {id: 'end', label: 'Poly'}, position: {x: 750, y: 100}},

      // Edges with initial values
      { data: { id: 'edge-sink-timer', source: 'sink', target: 'timer', value: "0", color: '#fff' } },
      { data: { id: 'edge-timer-wal', source: 'timer', target: 'wal', value: '0', color: '#fff' } },
      { data: { id: 'edge-wal-persister', source: 'wal', target: 'persister', value: '0', color: '#fff' } },

      {
        data: {id: 'edge-persister1', source: 'persister', target: 'persister1', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-persister2', source: 'persister', target: 'persister2', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-persister3', source: 'persister', target: 'persister3', value: '0', color: '#fff'},
        classes: "rotate"
      },

      {
        data: {id: 'edge-persister-nativer', source: 'persister', target: 'nativer', value: '0', color: '#fff'},
        classes: "no-label"
      },
      {
        data: {id: 'edge-nativer0', source: 'nativer', target: 'nativer0', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-nativer1', source: 'nativer', target: 'nativer1', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-nativer2', source: 'nativer', target: 'nativer2', value: '0', color: '#fff'},
        classes: "rotate"
      },

      {
        data: {id: 'edge-nativer-poly', source: 'nativer', target: 'end', value: '0', color: '#fff'},
        classes: "no-label"
      },
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
