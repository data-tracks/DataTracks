import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ArchitectureComponent } from './architecture.component';

describe('Architecture', () => {
  let component: ArchitectureComponent;
  let fixture: ComponentFixture<ArchitectureComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ArchitectureComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ArchitectureComponent);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
