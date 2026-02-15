import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CytoComponent } from './cyto.component';

describe('Cyto', () => {
  let component: CytoComponent;
  let fixture: ComponentFixture<CytoComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CytoComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CytoComponent);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
