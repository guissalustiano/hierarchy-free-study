use std::{collections::HashSet, fs::File, io::BufReader};
use valley_free::*;

fn main() {
    let asn = 15169; // Google

    let mut topo = Topology::new();
    let file = File::open("20231201.as-rel.txt").unwrap();

    let reader = BufReader::new(file);
    topo.build_topology(reader).unwrap();

    let mut all_paths = vec![];
    let mut seen = HashSet::new();
    topo.propagate_paths(&mut all_paths, asn, Direction::UP, vec![], &mut seen);

    dbg!(all_paths.len());
}
