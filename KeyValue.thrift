enum KeyValueStatus {
  OK = 1,
  ERROR = 2
}

enum ChordStatus {
  OK = 1,
  ERROR = 2
}

service KeyValueStore {
  string get(1: string key)

  ChordStatus create(),
  ChordStatus join(1: string existing_node_id),
  ChordStatus notify(1: string notifier_node_id),
  ChordStatus find_successor(1: string key_id),
  ChordStatus closest_preceding_node(1: string key_id),
  ChordStatus alive(),
  ChordStatus predecessor()

}

