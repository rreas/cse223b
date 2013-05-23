struct UserName {
  1: i32 uid,
  2: string name
}

service Adder {
  i64 add(1: i64 a, 2: i64 b)
}

