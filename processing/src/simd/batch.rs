use crate::simd::column::Column;

#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    pub(crate) columns: Vec<Column>,
    pub(crate) num_of_rows: usize,
}
