use crate::algebra::sort::Sort;
use crate::algebra::union::Union;
use crate::algebra::{
    Aggregate, Algebraic, Dual, Filter, IndexScan, Join, Project, Scan, VariableScan,
};

pub trait Visitor {
    fn visit(&mut self, alg: &Algebraic);
}

pub trait TypeVisitor {
    fn visit(&mut self, alg: &Algebraic) {
        match alg {
            Algebraic::Dual(d) => self.visit_dual(d),
            Algebraic::IndexScan(s) => self.visit_index_scan(s),
            Algebraic::Scan(s) => self.visit_scan(s),
            Algebraic::Project(p) => self.visit_project(p),
            Algebraic::Filter(f) => self.visit_filter(f),
            Algebraic::Join(j) => self.visit_join(j),
            Algebraic::Union(u) => self.visit_union(u),
            Algebraic::Aggregate(a) => self.visit_aggregate(a),
            Algebraic::Variable(v) => self.visit_variable_scan(v),
            Algebraic::Sort(s) => self.visit_sort(s),
        }
    }

    fn visit_dual(&mut self, dual: &Dual);

    fn visit_index_scan(&mut self, index_scan: &IndexScan);

    fn visit_scan(&mut self, scan: &Scan);

    fn visit_project(&mut self, project: &Project);

    fn visit_filter(&mut self, filter: &Filter);

    fn visit_join(&mut self, join: &Join);

    fn visit_union(&mut self, union: &Union);

    fn visit_aggregate(&mut self, aggregate: &Aggregate);

    fn visit_sort(&mut self, sort: &Sort);

    fn visit_variable_scan(&mut self, variable: &VariableScan);
}
