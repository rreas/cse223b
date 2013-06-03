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
  3: list<string> successor_list
}

struct GetValueResponse {
  1: ChordStatus status,
  2: string value
}

struct SuccessorListResponse {
  1: ChordStatus status,
  2: list<string> successor_list
}

struct ReplicasListResponse {
  1: ChordStatus status,
  2: list<string> replicate_list
}

service KeyValueStore {
  GetValueResponse get(1: string key),
  string get_predecessor(),
  string get_successor(),
  string get_successor_for_key(1: string key),
  SuccessorListResponse get_successor_list(),
  DataResponse get_init_data(1: string hash),
  ChordStatus put(1: string key, 2: string value),
  ChordStatus notify(1: string node),
  ChordStatus notify_predecessor(1: string node),
  ChordStatus move_backup(1: string node),
  void print_details(),
  void print_successor_list(),
  ChordStatus ping(),
  ChordStatus replicate(1: string key, 2: string value, 3: string source),
  ReplicasListResponse get_replicate_list(),
  i64 get_key_count()
}

