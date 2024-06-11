pub(crate) struct Block {

}


impl Block{
    pub(crate) fn default() -> Block{
        Block{}
    }
    pub(crate) fn parse(stencil: String) -> Block {
       Self::default()
    }
}