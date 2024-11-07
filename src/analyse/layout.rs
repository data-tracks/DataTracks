use crate::analyse::OutputDerivationStrategy::QueryBased;
use crate::language::Language;
use crate::processing::transform::build_algebra;
use crate::processing::Layout;
use std::collections::HashMap;
use OutputDerivationStrategy::{Combined, ContentBased, External, UserDefined};

pub trait InputDerivable {
    fn derive_input_layout(&self) -> Result<Layout, String>;
}

pub trait OutputDerivable {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Result<Layout, String>;
}


#[derive(Clone, Debug, PartialEq)]
pub enum OutputDerivationStrategy {
    QueryBased(QueryBasedStrategy),
    ContentBased,
    UserDefined(Layout),
    External,
    Combined(CombinedStrategy),
}

impl OutputDerivationStrategy {
    pub fn query_based(query: String, language: Language) -> Self {
        QueryBased(QueryBasedStrategy::new(query, language))
    }

    pub fn user_defined(layout: Layout) -> Self {
        UserDefined(layout)
    }

    pub fn combined(strategies: Vec<OutputDerivationStrategy>) -> Self {
        Combined(CombinedStrategy::new(strategies))
    }
}


impl OutputDerivable for OutputDerivationStrategy {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Result<Layout, String> {
        match self {
            QueryBased(strategy) => strategy.derive_output_layout(inputs),
            ContentBased => todo!(),
            UserDefined(layout) => Ok(layout.clone()),
            External => todo!(),
            Combined(comb) => comb.derive_output_layout(inputs),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryBasedStrategy {
    language: Language,
    query: String,
    layout: Layout,
}

impl QueryBasedStrategy {
    pub fn new(query: String, language: Language) -> Self {
        let algebra = build_algebra(&language, &query).unwrap();
        let layout = algebra.derive_output_layout(HashMap::new());
        QueryBasedStrategy { query, layout, language }
    }
}


impl OutputDerivable for QueryBasedStrategy {
    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Result<Layout, String> {
        Ok(self.layout.clone())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CombinedStrategy {
    strategies: Vec<OutputDerivationStrategy>,
}

impl CombinedStrategy {
    pub fn new(strategies: Vec<OutputDerivationStrategy>) -> Self {
        CombinedStrategy { strategies }
    }
}

impl OutputDerivable for CombinedStrategy {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Result<Layout, String> {
        Ok(self.strategies.iter().fold(Layout::default(), |a, b| a.merge(&b.derive_output_layout(inputs).unwrap()).unwrap()))
    }
}