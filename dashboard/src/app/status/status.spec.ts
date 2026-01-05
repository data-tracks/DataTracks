import {ComponentFixture, TestBed} from '@angular/core/testing';

import {Status} from './status';

describe('Status', () => {
    let component: Status;
    let fixture: ComponentFixture<Status>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [Status]
        })
            .compileComponents();

        fixture = TestBed.createComponent(Status);
        component = fixture.componentInstance;
        await fixture.whenStable();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
