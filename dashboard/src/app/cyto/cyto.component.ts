import {
  AfterViewInit,
  Component,
  effect,
  ElementRef,
  input,
  TemplateRef,
  ViewChild,
  ViewContainerRef
} from '@angular/core';
import cytoscape from 'cytoscape';
import cytoscapePopper from 'cytoscape-popper';
import tippy from 'tippy.js';

function tippyFactory(ref: any, content: any){
  // Since tippy constructor requires DOM element/elements, create a placeholder
  const dummyDomEle = document.createElement('div');

  return tippy(dummyDomEle, {
    getReferenceClientRect: ref.getBoundingClientRect,
    trigger: 'manual', // mandatory
    // dom element inside the tippy:
    content: content,
    // your own preferences:
    arrow: true,
    placement: 'top',
    hideOnClick: false,
    sticky: "reference",

    // if interactive:
    interactive: true,
    appendTo: document.body // or append dummyDomEle to document.body
  });
}

cytoscape.use(cytoscapePopper(tippyFactory));

@Component({
  selector: 'app-cyto',
  standalone: true,
  templateUrl: `./cyto.component.html`,
})
export class CytoComponent implements AfterViewInit {
  @ViewChild('cyContainer') container!: ElementRef;
  inputs = input.required<any>();
  @ViewChild('tooltipTemplate') tooltipTemplate!: TemplateRef<any>;

  private cy?: cytoscape.Core;
  private queues = new Map<string, number>();

  protected wal = new Map<number, number>();
  private viewContainerRef: ViewContainerRef;

  constructor(private vcr: ViewContainerRef) {
    this.viewContainerRef  = vcr;
    effect(() => {
      const entry = this.inputs();
      if (!entry) return;

      if (entry.name.toLowerCase().includes("wal delayed")) {
        let id = entry.name.toLowerCase().replace("wal delayed", "").trim() as number;
        this.wal.set(id, entry.size)
      }

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
        },
        {
          selector: 'node.database', // Triggered if node has 'database' class
          style: {
            'background-image': 'assets/database.png', // #2085b5
            'background-fit': 'contain',
            'background-clip': 'none',
            'width': 50,
            'height': 50,
            'label': 'data(label)',
            'background-opacity': 0, // Makes the default circle/square invisible
            'shape': 'rectangle'    // Gives the image a frame to sit in
          }
        },
        {
          selector: 'node.save',
          style: {
            'background-image': 'assets/save.png', // #2085b5
            'background-fit': 'contain',
            'background-clip': 'none',
            'width': 40,
            'height': 40,
            'label': 'data(label)',
            'background-opacity': 0, // Makes the default circle/square invisible
            'shape': 'rectangle'    // Gives the image a frame to sit in
          }
        },
        {
          selector: 'node.in', // Triggered if node has 'database' class
          style: {
            'background-image': 'assets/in.png', // #2085b5
            'background-fit': 'contain',
            'background-clip': 'none',
            'width': 30,
            'height': 30,
            'label': 'data(label)',
            'background-opacity': 0, // Makes the default circle/square invisible
            'shape': 'round-rectangle',  // Gives the image a frame to sit in
          }
        }
      ],
      elements: this.getInitialElements(),
      layout: {
        name: 'preset'
      }
    });

    this.setupPopups();
  }

  setupPopups() {
    if (!this.cy){
      return
    }

    const embeddedView = this.viewContainerRef.createEmbeddedView(this.tooltipTemplate, {
      wals: Array.from(this.wal)
    });

    this.cy.edges(".wal").forEach(node => {
      // 1. Create a popper reference
      const ref = node.popperRef();

      // 2. Initialize Tippy on a dummy element and link it to the reference
      const dummyDomEle = document.createElement('div');
      const tip = tippy(dummyDomEle, {
        getReferenceClientRect: ref.getBoundingClientRect, // Link position to node
        content: embeddedView.rootNodes[0],//`Details for ${node.data('id')}`,
        trigger: 'manual', // We will trigger it via Cytoscape events
        interactive: true,
        arrow: true,
        //theme: 'cytoscape-popper',
        appendTo: () => document.body // Ensures popup isn't clipped by container
      });

      // 3. Attach listeners to show/hide
      node.on('mouseover', () => tip.show());
      node.on('mouseout', () => tip.hide());
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
    this.cy.batch(() => {
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
      const element = this.cy?.getElementById("edge-wal-delay");
      if (element) {
        let value = Array.from(this.wal.values()).reduce((acc, value) => acc + value, 0);
        // Update data without refreshing layout
        element.data('value', this.formatNumber(value));
        element.data('color', this.getQueueColor(value));
      }
    })

  }

  private getInitialElements(): cytoscape.ElementDefinition[] {
    let x = 0;
    let y = 200;
    let xDistance = 150;
    let yDistance = 200;

    let xPersisterDistance = x + xDistance*3
    let xNativerDistance = x + xDistance*4
    let xEnd = x + xDistance*5

    let yFirst = yDistance + 200;

    let ySecond = yFirst + 100;

    return [
      // Nodes
      {data: {id: 'sink', label: 'Sink'}, position: {x: x, y: y}},
      {data: {id: 'sinkLogo', label: ''}, position: {x: x-40, y: y}, classes:"in" },
      {data: {id: 'timer', label: 'Timer'}, position: {x: x + xDistance, y: y}},
      {data: {id: 'wal', label: 'WAL'}, position: {x: x + xDistance*2, y: y}},

      // WAL persister
      {data: {id: 'walBuffer', label: 'WAL Delay'}, position: {x: x + xDistance*2, y: 50}, classes: "save"},

      // Persisters
      {data: {id: 'persister', label: 'Persister'}, position: {x: xPersisterDistance, y: y}},
      {data: {id: 'persister1', label: 'Mongo'}, position: {x: xPersisterDistance - 50, y: yFirst}, classes: "database"},
      {data: {id: 'persister2', label: 'Postgres'}, position: {x: xPersisterDistance, y: yFirst}, classes: "database"},
      {data: {id: 'persister3', label: 'Neo4j'}, position: {x: xPersisterDistance + 50, y: yFirst}, classes: "database"},

      // Nativers
      {data: {id: 'nativer', label: 'Nativer'}, position: {x: xNativerDistance, y: y}},
      {data: {id: 'nativer0', label: 'Definition 0'}, position: {x: xNativerDistance - 50, y: yFirst}, classes: "rotate"},
      {data: {id: 'nativer1', label: 'Definition 1'}, position: {x: xNativerDistance, y: yFirst}, classes: "rotate"},
      {data: {id: 'nativer2', label: 'Definition 2'}, position: {x: xNativerDistance + 50, y: yFirst}, classes: "rotate"},

      {data: {id: 'nativer0engine', label: 'Mongo'}, position: {x: xNativerDistance - 50, y: ySecond}, classes: "database"},
      {data: {id: 'nativer1engine', label: 'Postgres'}, position: {x: xNativerDistance, y: ySecond}, classes: "database"},
      {data: {id: 'nativer2engine', label: 'Neo4j'}, position: {x: xNativerDistance + 50, y: ySecond}, classes: "database"},

      // end
      {data: {id: 'end', label: 'Poly'}, position: {x: xEnd, y: y}},
      {data: {id: 'endLogo', label: ''}, position: {x: xEnd + 40, y: y}, classes:"in" },

      // Edges with initial values
      { data: { id: 'edge-sink-timer', source: 'sink', target: 'timer', value: "0", color: '#fff' } },
      { data: { id: 'edge-timer-wal', source: 'timer', target: 'wal', value: '0', color: '#fff' } },
      { data: { id: 'edge-wal-delay', source: 'wal', target: 'walBuffer', value: '0', color: '#fff' }, classes: "wal" },
      { data: { id: 'edge-wal-delay-back', source: 'walBuffer', target: 'wal', value: ' ', color: '#fff' } },
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
      //nativer
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

      //nativer engines
      {
        data: {id: 'edge-nativer0engine', source: 'nativer0', target: 'nativer0engine', value: '0', color: '#fff'},
        classes: "rotate no-label"
      },
      {
        data: {id: 'edge-nativer1engine', source: 'nativer1', target: 'nativer1engine', value: '0', color: '#fff'},
        classes: "rotate no-label"
      },
      {
        data: {id: 'edge-nativer2engine', source: 'nativer2', target: 'nativer2engine', value: '0', color: '#fff'},
        classes: "rotate no-label"
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
