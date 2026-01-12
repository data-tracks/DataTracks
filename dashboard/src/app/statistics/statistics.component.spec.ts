import {ComponentFixture, TestBed} from '@angular/core/testing';

import {StatisticsComponent} from './statistics.component';

describe('Statistics', () => {
    let component: StatisticsComponent;
    let fixture: ComponentFixture<StatisticsComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [StatisticsComponent]
        })
            .compileComponents();

        fixture = TestBed.createComponent(StatisticsComponent);
        component = fixture.componentInstance;
        await fixture.whenStable();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
