@0xdbb8ad1d25721831; # Unique ID for the file

struct Value {
  union {
    int @0 :Int64;
    float :group {
      number @1 :Int64;
      shift @2 :UInt8;
    }
    bool @3 :Bool;
    text @4 :Text;
    time :group {
      ms @5 :Int64;
      ns @6 :UInt32;
    }
    date @7 :Int64;
    array @8 :List(Value);
    dict @9 :List(Entry);
    node @10 :Node;
    edge @11 :Edge;
    null @12 :Void;
  }

  struct Entry {
    key @0 :Text;
    value @1 :Value;
  }
}

struct Node {
  id @0 :Int64;
  labels @1 :List(Text);
  properties @2 :List(Value.Entry);
}

struct Edge {
  label @0 :Text;
  startId @1 :Int64;
  endId @2 :Int64;
  properties @3 :List(Value.Entry);
}