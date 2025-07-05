use crate::algebra;
use crate::algebra::Algebraic::{Aggregate, Dual, IndexScan, Join, Scan, Variable};
use crate::algebra::{Algebra, Algebraic, BoxedIterator, Filter, Operator, Project, VariableScan};
use crate::optimize::Cost;
use crate::processing::Layout;
use std::collections::{HashMap, HashSet};
use value::Value;

#[derive(Clone, Debug)]
pub struct AlgebraRoot {
    nodes: HashMap<usize, Algebraic>,
    connection: HashMap<usize, HashSet<usize>>,
    aliases: HashMap<usize, String>,
    count: usize,
    ends: Vec<usize>,
}

pub trait AlgInputDerivable {
    fn derive_input_layout(&self, root: &AlgebraRoot) -> Option<Layout>;
}

pub trait AlgOutputDerivable {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        root: &AlgebraRoot,
    ) -> Option<Layout>;
}

impl AlgebraRoot {
    pub(crate) fn calc_cost(&self) -> Cost {
        todo!()
    }

    pub(crate) fn remove_set(&self) {
        todo!()
    }

    pub(crate) fn add_set(&self) {
        todo!()
    }

    pub fn add_children(&mut self, id: usize, children: Vec<Algebraic>) {
        children.iter().for_each(|child| {
            self.nodes.insert(id, child.clone());
        })
    }
    
    pub fn get_node(&self, id: usize) -> Option<&Algebraic> {
        self.nodes.get(&id)
    }

    pub(crate) fn append(&mut self, other: AlgebraRoot) {
        // mapping new to old is
        let mut mappings = HashMap::new();
        // inserts other nodes and give new ids
        for (id, node) in other.nodes {
            let new_id = self.new_id();
            let node = node.replace_id(new_id);
            self.nodes.insert(new_id, node);
            mappings.insert(id, new_id);
        }

        // move connections
        for old_id in mappings.keys() {
            let id = mappings.get(old_id).unwrap().clone();
            self.connection.insert(
                id,
                other
                    .connection
                    .get(old_id)
                    .unwrap()
                    .into_iter()
                    .map(|id| mappings.get(id).unwrap().clone())
                    .collect(),
            );
        }

        self.ends.append(
            &mut other
                .ends
                .iter()
                .map(|id| mappings.get(id).unwrap().clone())
                .collect(),
        );
    }

    pub(crate) fn pop(&mut self) -> Result<Algebraic, String> {
        let end = self.ends.pop().unwrap();

        if self.ends.is_empty() {
            let ends = self
                .connection
                .get(&end)
                .cloned()
                .ok_or(String::from("Connection not found"))?;
            self.ends = ends.into_iter().collect();
        }
        self.nodes
            .get(&end)
            .map(|a| a as &Algebraic)
            .cloned()
            .ok_or(String::from("Node not found"))
    }

    pub(crate) fn join_cross(&mut self) {
        self.join_hash(
            |_v| Value::bool(true),
            |_v| Value::bool(true),
            |l, r| Value::array(vec![l, r]),
        );
    }

    pub(crate) fn join_natural(&mut self) {
        self.join_hash(
            |val| val.clone(),
            |val| val.clone(),
            |left, right| Value::Dict(left.as_dict().unwrap().merge(right.as_dict().unwrap())),
        );
    }

    pub(crate) fn join_hash(
        &mut self,
        hash_left: fn(&Value) -> Value,
        hash_right: fn(&Value) -> Value,
        out: fn(Value, Value) -> Value,
    ) {
        let mut id = self.ends.remove(0);
        while !self.ends.is_empty() {
            let new_id = self.new_id();
            let right = self.ends.remove(0);
            self.nodes.insert(
                id,
                Join(algebra::Join::new(new_id, hash_left, hash_right, out)),
            );
            self.connection.insert(id, HashSet::from([id, right]));
            id = new_id;
        }
        self.ends.push(id);
    }

    pub(crate) fn ends(&self) -> &Vec<usize> {
        &self.ends
    }

    pub(crate) fn aliases(&self) -> Vec<String> {
        self.aliases.values().cloned().collect()
    }

    pub(crate) fn variable(&mut self, name: String) {
        let id = self.new_id();
        self.nodes.insert(id, Variable(VariableScan::new(id, name)));
        self.connection.insert(
            id,
            HashSet::from_iter(std::mem::take(&mut self.ends).into_iter()),
        );
        self.ends.push(id);
    }

    pub(crate) fn alias(&mut self, name: String) {
        let last = self.nodes.get(self.ends.last().unwrap()).unwrap();
        self.aliases.insert(last.id(), name);
    }

    pub(crate) fn dual() -> Self {
        Self::new(Dual(algebra::Dual::new(0)))
    }

    pub(crate) fn new_scan_index(index: usize) -> Self {
        Self::new(IndexScan(algebra::IndexScan::new(0, index)))
    }

    pub(crate) fn scan_index(&mut self, index: usize) {
        let id = self.new_id();
        IndexScan(algebra::IndexScan::new(id, index));
        self.ends.push(id);
    }

    pub(crate) fn new_scan_name<P: AsRef<str>>(name: P) -> Self {
        Self::new(Scan(algebra::Scan::new(name.as_ref().to_string(), 0)))
    }

    pub(crate) fn aggregate(&mut self, function: Operator, group: Option<Operator>) {
        let id = self.new_id();
        self.nodes
            .insert(id, Aggregate(algebra::Aggregate::new(id, function, group)));
        self.connection.insert(
            id,
            HashSet::from_iter(std::mem::take(&mut self.ends).into_iter()),
        );
        self.ends.push(id)
    }

    pub fn project(&mut self, project: Operator) {
        let id = self.new_id();
        self.nodes
            .insert(id, Algebraic::Project(Project::new(id, project)));
        self.connection.insert(
            id,
            HashSet::from_iter(std::mem::take(&mut self.ends).into_iter()),
        );
        self.ends.push(id);
    }

    pub fn filter(&mut self, condition: Operator) {
        let id = self.new_id();
        self.nodes
            .insert(id, Algebraic::Filter(Filter::new(id, condition)));
        self.connection.insert(
            id,
            HashSet::from_iter(std::mem::take(&mut self.ends).into_iter()),
        );
        self.ends.push(id);
    }

    pub fn new(algebra: Algebraic) -> Self {
        let id = algebra.id();
        AlgebraRoot {
            nodes: HashMap::from([(id, algebra)]),
            connection: Default::default(),
            aliases: Default::default(),
            count: id,
            ends: vec![id],
        }
    }

    pub(crate) fn derive_iterator(&mut self) -> Result<BoxedIterator, String> {
        self.nodes
            .get(self.ends.last().ok_or("Algebraic root is empty")?)
            .ok_or(String::from("Could not find node"))?
            .derive_iterator(self)
    }

    pub fn get_child(&self, id: usize) -> Option<&Algebraic> {
        self.nodes.get(&id)
    }

    pub(crate) fn get_children(&self, id: usize) -> Vec<&Algebraic> {
        let children = self.connection.get(&id).cloned().unwrap_or_default();
        children
            .iter()
            .map(|id| self.nodes.get(id).unwrap())
            .collect()
    }

    pub fn add_node(&mut self, node: Algebraic) {
        self.nodes.insert(node.id(), node);
    }

    pub fn new_id(&mut self) -> usize {
        let id = self.count;
        self.count += 1;
        id
    }

    pub(crate) fn derive_input_layout(&self) -> Option<Layout> {
        self.nodes.get(self.ends.last()?)?.derive_input_layout(self)
    }

    pub fn derive_output_layout(&self, inputs: HashMap<String, Layout>) -> Option<Layout> {
        self.nodes
            .get(self.ends.last()?)?
            .derive_output_layout(inputs, self)
    }
}

impl FromIterator<AlgebraRoot> for Option<AlgebraRoot> {
    fn from_iter<T: IntoIterator<Item = AlgebraRoot>>(iter: T) -> Self {
        let mut iter = iter.into_iter();

        let mut first = iter.next()?;
        while let Some(root) = iter.next() {
            first.append(root);
        }
        Some(first)
    }
}
