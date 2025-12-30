pub enum DefinitionTarget {
    Graph(NodeSource, EdgeSource),
    Document(DocumentSource),
    Relational(RowSource),
}

pub enum NodeSource {
    Document{id: DocumentSource, label: DocumentSource, properties: DocumentSource},
    Node
}

pub enum DocumentSource {
    Key(String),
    Whole
}

pub enum EdgeSource {
    Document{id: DocumentSource, label: DocumentSource, properties: DocumentSource, source: DocumentSource, target: DocumentSource},
    Edge
}

pub enum RowSource{
    Document(DocumentSource),
    List
}

