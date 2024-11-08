use crate::analyse::OutputDerivationStrategy::{QueryBased, Undefined};
use crate::language::Language;
use crate::processing::transform::build_algebra;
use crate::processing::Layout;
use std::collections::HashMap;
use OutputDerivationStrategy::{Combined, ContentBased, External, UserDefined};

pub trait InputDerivable {
    fn derive_input_layout(&self) -> Option<Layout>;
}

pub trait OutputDerivable {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout>;
}


#[derive(Clone, Debug, PartialEq)]
pub enum OutputDerivationStrategy {
    QueryBased(QueryBasedStrategy),
    ContentBased,
    UserDefined(Layout),
    External,
    Combined(CombinedStrategy),
    Undefined
}

impl Default for OutputDerivationStrategy {
    fn default() -> Self {
        Undefined
    }
}

impl OutputDerivationStrategy {
    pub fn query_based(query: String, language: Language) -> Result<Self, String> {
        Ok(QueryBased(QueryBasedStrategy::new(query, language)?))
    }

    pub fn user_defined(layout: Layout) -> Self {
        UserDefined(layout)
    }

    pub fn combined(strategies: Vec<OutputDerivationStrategy>) -> Self {
        Combined(CombinedStrategy::new(strategies))
    }
}


impl OutputDerivable for OutputDerivationStrategy {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        match self {
            QueryBased(strategy) => strategy.derive_output_layout(inputs),
            ContentBased => todo!(),
            UserDefined(layout) => Some(layout.clone()),
            External => todo!(),
            Combined(comb) => comb.derive_output_layout(inputs),
            Undefined => Some(Layout::default())
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
    pub fn new(query: String, language: Language) -> Result<Self, String> {
        let algebra = build_algebra(&language, &query)?;
        let layout = algebra.derive_output_layout(HashMap::new()).ok_or("Could not derive layout.")?;
        Ok(QueryBasedStrategy { query, layout, language })
    }
}


impl OutputDerivable for QueryBasedStrategy {
    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Option<Layout> {
        Some(self.layout.clone())
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
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        Some(self.strategies.iter().fold(Layout::default(), |a, b| a.merge(&b.derive_output_layout(inputs.clone()).unwrap())))
    }
}

#[cfg(test)]
mod tests {
    use crate::analyse::{OutputDerivable, OutputDerivationStrategy};
    use crate::language::Language;
    use crate::processing::transform::build_algebra;
    use crate::processing::Layout;
    use std::collections::HashMap;

    #[test]
    fn test_simple_layout_single() {
        let strategy = OutputDerivationStrategy::query_based("SELECT \"id\" FROM \"company\" WHERE \"name\" = $".to_string(), Language::Sql).unwrap();
        let output = strategy.derive_output_layout(HashMap::new()).unwrap();
        assert_eq!(Layout::default(), output);
    }

    #[test]
    fn test_simple_layout_array() {
        let strategy = OutputDerivationStrategy::query_based("SELECT \"id\", \"name\" FROM \"company\" WHERE \"name\" = $".to_string(), Language::Sql).unwrap();
        let output = strategy.derive_output_layout(HashMap::new()).unwrap();
        assert_eq!(Layout::tuple(vec![Some("id".to_string()), Some("name".to_string())]), output);
        assert_ne!(Layout::default(), output);
    }

    #[test]
    fn test_simple_layout_dic_alg() {
        let node = build_algebra(&Language::Sql, &"SELECT {\"id\":\"id\", \"name\":\"name\"} FROM \"company\"".to_string()).unwrap();
        let output = node.derive_output_layout(HashMap::new()).unwrap();
        assert_eq!(Layout::dict(vec!["id".to_string(), "name".to_string()]), output);
        assert_ne!(Layout::default(), output);
    }

    #[test]
    fn test_simple_layout_dic() {
        let strategy = OutputDerivationStrategy::query_based("SELECT {'id':\"id\", 'name':\"name\"} FROM \"company\" WHERE \"name\" = $".to_string(), Language::Sql).unwrap();
        let output = strategy.derive_output_layout(HashMap::new()).unwrap();
        assert_eq!(Layout::dict(vec!["id".to_string(), "name".to_string()]), output);
        assert_ne!(Layout::default(), output);
    }
}