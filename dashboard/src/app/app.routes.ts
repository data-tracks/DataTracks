import {Routes} from '@angular/router';
import {StatisticsComponent} from "./statistics/statistics.component";
import EventsComponent from "./events/events.component";
import {CytoComponent} from "./cyto/cyto.component";
import {ThreadsComponent} from "./threads/threads.component";
import {RoundtripComponent} from "./roundtrip/roundtrip.component";

export const routes: Routes = [
    { path: 'stats', component: StatisticsComponent },
    { path: 'events', component: EventsComponent },
    { path: 'queues', component: CytoComponent },
    { path: 'threads', component: ThreadsComponent },
    { path: 'roundtrip', component: RoundtripComponent },
    { path: '', redirectTo: 'stats', pathMatch: 'full' } // Default tab
];
