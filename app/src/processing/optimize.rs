use crate::optimize::OptimizeStrategy;
use core::util::iterator::BoxedValueIterator;
use std::collections::HashMap;
use track_rails::message_generated::protocol::Transform;

pub trait Optimizable {
    fn optimize(
        &self,
        transforms: HashMap<String, Transform>,
        optimizer: Option<OptimizeStrategy>,
    ) -> BoxedValueIterator;
}