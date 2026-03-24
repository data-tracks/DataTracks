use crate::simd::column::Column;

#[derive(Clone, Debug)]
pub struct RecordBatch {
    pub(crate) columns: Vec<Column>,
    pub(crate) num_of_rows: usize,
}
