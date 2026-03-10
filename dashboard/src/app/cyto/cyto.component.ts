import {
  AfterViewInit,
  ChangeDetectorRef,
  Component,
  effect,
  ElementRef,
  inject,
  TemplateRef,
  ViewChild,
  ViewContainerRef
} from '@angular/core';
import cytoscape from 'cytoscape';
import cytoscapePopper from 'cytoscape-popper';
import tippy from 'tippy.js';
import {EventsService} from "../events.service";

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
  @ViewChild('tooltipTemplate') tooltipTemplate!: TemplateRef<any>;

  service = inject(EventsService);

  private cy?: cytoscape.Core;
  private queues = new Map<string, number>();

  protected wal = new Map<number, number>();

  protected engines = new Map<string, number>();

  protected enginesBuffer = new Map<string, number>();
  protected enginesBufferFile = new Map<string, number>();

  private viewContainerRef: ViewContainerRef;
  private changeDetectorRef: ChangeDetectorRef;

  constructor(private vcr: ViewContainerRef, private cd: ChangeDetectorRef) {
    this.service.initQueueConnection();
    this.changeDetectorRef = cd;
    this.viewContainerRef  = vcr;
    effect(() => {
      const entry = this.service.queues();
      if (!entry) return;
      //console.log(entry)

      const name =  entry.name.toLowerCase();
      if (name.includes("wal delayed")) {
        let id = name.replace("wal delayed", "").trim() as number;
        this.wal.set(id, entry.size)
      }

      if (name.includes("engine") && !name.includes("engines")) {
        let id = name.replace("engine-", "").trim() as string;

        if (id.includes("buffer")){
          this.enginesBuffer.set(id.replace("-buffer", ""), entry.size);
        }else {
          this.engines.set(id, entry.size);
        }
      }
      if (name.includes("persister-file")) {
        let id = name.replace("persister-file-");
        this.enginesBufferFile.set(id, entry.size);
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
          selector: 'node.rotate',
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
          selector: 'node.buffer', // Triggered if node has 'database' class
          style: {
            'background-image': 'assets/buffer.png', // #2085b5
            'background-fit': 'contain',
            'background-clip': 'none',
            'width': 30,
            'height': 30,
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
    this.cd.detectChanges();
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
      // unified wal delay
      const element = this.cy?.getElementById("edge-wal-delay");
      if (element) {
        let value = Array.from(this.wal.values()).reduce((acc, value) => acc + value, 0);
        // Update data without refreshing layout
        element.data('value', this.formatNumber(value));
        element.data('color', this.getQueueColor(value));
      }

      for (let engine of this.engines) {
        // out
        let name = engine[0].split("-")[0];

        const element = this.cy?.getElementById(`edge-${name}`);
        if (element) {
          let value = Array.from(this.engines).filter(([key]) => key.includes(name)).map(([, value]) => value).reduce((acc, value) => acc + value, 0);
          // Update data without refreshing layout
          element.data('value', this.formatNumber(value));
          element.data('color', this.getQueueColor(value));
        }
        // buffers
        const buffer = this.cy?.getElementById(`edge-buffer-${name}`);
        if (buffer) {
          let value = Array.from(this.enginesBuffer).filter(([key]) => key.includes(name)).map(([, value]) => value).reduce((acc, value) => acc + value, 0);
          buffer.data('value', this.formatNumber(value));
          buffer.data('color', this.getQueueColor(value));
        }
        // file buffers
        const fileBuffer = this.cy?.getElementById(`buffer-${name}`);
        if (fileBuffer) {
          let value = Array.from(this.enginesBufferFile).filter(([key]) => key.includes(name)).map(([, value]) => value).reduce((acc, value) => acc + value, 0);
          fileBuffer.data('label', this.formatNumber(value));
          fileBuffer.data('color', this.getQueueColor(value));
        }

      }
    })
    this.cd.detectChanges();
  }

  private getInitialElements(): cytoscape.ElementDefinition[] {
    let x = 0;
    let y = 200;
    let xDistance = 150;
    let yDistance = 200;

    let xPersiste = x + xDistance*3
    let xNative = x + xDistance*4
    let xProcess = x + xDistance*5
    let xEnd = x + xDistance*6

    let yFirst = yDistance + 200;

    let ySecond = yFirst + 150;

    return [
      // Nodes
      {data: {id: 'sink', label: 'Sink'}, position: {x: x, y: y}},
      {data: {id: 'sinkLogo', label: ''}, position: {x: x-40, y: y}, classes:"in" },
      {data: {id: 'timer', label: 'Timer'}, position: {x: x + xDistance, y: y}},
      {data: {id: 'wal', label: 'WAL'}, position: {x: x + xDistance*2, y: y}},

      // WAL persister
      {data: {id: 'walBuffer', label: 'WAL Delay'}, position: {x: x + xDistance*2, y: 50}, classes: "save"},

      // Persisters
      {data: {id: 'persister', label: 'Persist'}, position: {x: xPersiste, y: y}},

      // buffers
      {
        data: {id: 'buffer-mongodb', label: '0'},
        position: {x: xPersiste - 50, y: yFirst},
        classes: "buffer rotate"
      },
      {
        data: {id: 'buffer-postgres', label: '0'},
        position: {x: xPersiste, y: yFirst},
        classes: "buffer rotate"
      },
      {
        data: {id: 'buffer-neo4j', label: '0'},
        position: {x: xPersiste + 50, y: yFirst},
        classes: "buffer rotate"
      },

      {
        data: {id: 'engine-mongodb', label: 'Mongo'},
        position: {x: xPersiste - 50, y: ySecond},
        classes: "database"
      },
      {
        data: {id: 'engine-postgres', label: 'Postgres'},
        position: {x: xPersiste, y: ySecond},
        classes: "database"
      },
      {
        data: {id: 'engine-neo4j', label: 'Neo4j'},
        position: {x: xPersiste + 50, y: ySecond},
        classes: "database"
      },

      // Nativers
      {data: {id: 'nativer', label: 'Native'}, position: {x: xNative, y: y}},
      {data: {id: 'nativer0', label: 'Definition 0'}, position: {x: xNative - 50, y: yFirst}, classes: "rotate"},
      {data: {id: 'nativer1', label: 'Definition 1'}, position: {x: xNative, y: yFirst}, classes: "rotate"},
      {data: {id: 'nativer2', label: 'Definition 2'}, position: {x: xNative + 50, y: yFirst}, classes: "rotate"},

      {data: {id: 'nativer0engine', label: 'Mongo'}, position: {x: xNative - 50, y: ySecond}, classes: "database"},
      {data: {id: 'nativer1engine', label: 'Postgres'}, position: {x: xNative, y: ySecond}, classes: "database"},
      {data: {id: 'nativer2engine', label: 'Neo4j'}, position: {x: xNative + 50, y: ySecond}, classes: "database"},

      // Processor
      {data: {id: 'processor', label: 'Process'}, position: {x: xProcess, y: y}},
      {data: {id: 'processor0', label: 'Definition 0'}, position: {x: xProcess - 50, y: yFirst}, classes: "rotate"},
      {data: {id: 'processor1', label: 'Definition 1'}, position: {x: xProcess, y: yFirst}, classes: "rotate"},
      {data: {id: 'processor2', label: 'Definition 2'}, position: {x: xProcess + 50, y: yFirst}, classes: "rotate"},

      {data: {id: 'processor0engine', label: 'Mongo'}, position: {x: xProcess - 50, y: ySecond}, classes: "database"},
      {data: {id: 'processor1engine', label: 'Postgres'}, position: {x: xProcess, y: ySecond}, classes: "database"},
      {data: {id: 'processor2engine', label: 'Neo4j'}, position: {x: xProcess + 50, y: ySecond}, classes: "database"},

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
        data: {id: 'edge-buffer-mongodb', source: 'persister', target: 'buffer-mongodb', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-buffer-postgres', source: 'persister', target: 'buffer-postgres', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-buffer-neo4j', source: 'persister', target: 'buffer-neo4j', value: '0', color: '#fff'},
        classes: "rotate"
      },


      {
        data: {id: 'edge-mongodb', source: 'buffer-mongodb', target: 'engine-mongodb', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-postgres', source: 'buffer-postgres', target: 'engine-postgres', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-neo4j', source: 'buffer-neo4j', target: 'engine-neo4j', value: '0', color: '#fff'},
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
      // Processor
      {
        data: {id: 'edge-nativer-processor', source: 'nativer', target: 'processor', value: '0', color: '#fff'},
        classes: "no-label"
      },
      {
        data: {id: 'edge-process0', source: 'processor', target: 'processor0', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-process1', source: 'processor', target: 'processor1', value: '0', color: '#fff'},
        classes: "rotate"
      },
      {
        data: {id: 'edge-process2', source: 'processor', target: 'processor2', value: '0', color: '#fff'},
        classes: "rotate"
      },
      // Processor engines
      {
        data: {id: 'edge-process0engine', source: 'processor0', target: 'processor0engine', value: '0', color: '#fff'},
        classes: "rotate no-label"
      },
      {
        data: {id: 'edge-process1engine', source: 'processor1', target: 'processor1engine', value: '0', color: '#fff'},
        classes: "rotate no-label"
      },
      {
        data: {id: 'edge-process2engine', source: 'processor2', target: 'processor2engine', value: '0', color: '#fff'},
        classes: "rotate no-label"
      },
      // end
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
