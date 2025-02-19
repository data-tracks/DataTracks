
pub trait  Planner {
    /// Takes a [`WorkloadPlan`] and separates into execution steps,
    /// in the end it creates an initial [`ExecutionPlan`]
    fn plan(&self, plan: WorkloadPlan) -> &ExecutionPlan;
}


pub trait Engine {

    fn plans(&self) -> &Vec<ExecutionPlan>;
    fn running_plans(&self) -> &Vec<ExecutionPlan>;
    fn stopped_plans(&self) -> &Vec<ExecutionPlan>;

    /// Takes an [`ExecutionPlan`] and executes it
    fn execute(&self, plan: ExecutionPlan);


}



pub struct ExecutionPlan {}


pub trait Executable {

    // fn execute(&self);

    fn needs_changes(&self) -> bool;
}



pub struct WorkloadPlan {}