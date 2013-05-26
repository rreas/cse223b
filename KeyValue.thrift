enum KeyValueStatus {
  OK = 1,
  ERROR = 2
}

enum ChordStatus {
  OK = 1,
  ERROR = 2
}

struct DataResponse {
  1: ChordStatus status,
  2: map<string, string> kvstore,
  3: list<string> finger_node_table,
  4: list<string> finger_hash_table
}

struct GetValueResponse {
  1: ChordStatus status,
  2: string value
}

service KeyValueStore {
  GetValueResponse get(1: string key),
  string get_predecessor(),
  string get_successor_for_key(1: string key),
  DataResponse get_init_data(1: string hash),
  ChordStatus put(1: string key, 2: string value),
  ChordStatus notify(1: string node)
  ChordStatus replicate(1: string key, 2: string value, 3: string source)
}

