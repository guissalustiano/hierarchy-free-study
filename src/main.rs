use once_cell::sync::Lazy;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter, Write},
};
use valley_free::*;

static TIER1_ASNS: [u32; 15] = [
    7018, 3320, 3257, 6830, 3356, 2914, 5511, 3491, 1239, 6453, 6762, 1299, 12956, 701, 6461,
];

static TIER2_ASNS: [u32; 32] = [
    6939, 7713, 9002, 1764, 34549, 4766, 9304, 22652, 9318, 3292, 2497, 1273, 2516, 23947, 4134,
    4809, 4837, 3462, 5400, 7922, 1257, 12390, 2711, 8002, 14744, 38930, 33891, 41327, 7473, 24482,
    9121, 6663,
];

static CLOUD_PROVIDERS: [u32; 6] = [
    36351, // IBM
    19604, // IBM Cloud
    15169, // Google
    8075,  // Microsoft (Not azure)
    12076, // Microsoft (Azure)
    16509, // Amazon Cloud
];

static TOPOLOGY: Lazy<Topology> = Lazy::new(|| {
    let mut topo = Topology::new();
    let file = File::open("20231201.as-rel.txt").unwrap();
    let reader = BufReader::new(file);

    topo.build_topology(reader).unwrap();
    topo
});

fn propagate_paths(asn: u32) -> Vec<Path> {
    let mut all_paths = vec![];
    let mut seen = HashSet::new();
    TOPOLOGY.propagate_paths(&mut all_paths, asn, Direction::UP, vec![], &mut seen);
    all_paths
}

struct DataRecord {
    asn: u32,
    provider_free: usize,
    tier1_free: usize,
    hierachy_free: usize,
    type_: AsType,
}

enum AsType {
    Tier1,
    Tier2,
    CloudProvider,
    Other,
}

impl AsType {
    fn to_str(&self) -> &str {
        match self {
            AsType::Tier1 => "tier1",
            AsType::Tier2 => "tier2",
            AsType::CloudProvider => "cloud_provider",
            AsType::Other => "other",
        }
    }
}

fn classify_asn(asn: u32) -> AsType {
    if TIER1_ASNS.contains(&asn) {
        AsType::Tier1
    } else if TIER2_ASNS.contains(&asn) {
        AsType::Tier2
    } else if CLOUD_PROVIDERS.contains(&asn) {
        AsType::CloudProvider
    } else {
        AsType::Other
    }
}

fn count_hierachy_free_paths(topo: &Topology, asn: u32) -> DataRecord {
    let autonomo_system = topo.ases_map.get(&asn).unwrap();
    let providers: HashSet<_> = autonomo_system
        .providers
        .iter()
        .copied()
        .filter(|&x| x != asn)
        .collect();

    let tiers1: HashSet<_> = TIER1_ASNS.iter().copied().filter(|&x| x != asn).collect();
    let tiers2: HashSet<_> = TIER2_ASNS.iter().copied().filter(|&x| x != asn).collect();

    let all_paths = propagate_paths(asn);
    let provider_free = all_paths
        .iter()
        .filter(|path| !providers.iter().any(|&provider| path.contains(&provider)));

    let provider_free_count = provider_free.clone().count();

    let tier1_free =
        provider_free.filter(|path| !tiers1.iter().any(|&tier1| path.contains(&tier1)));

    let tier1_free_count = tier1_free.clone().count();

    let hierachy_free =
        tier1_free.filter(|path| !tiers2.iter().any(|&tier2| path.contains(&tier2)));

    let hierachy_free_count = hierachy_free.count();

    DataRecord {
        asn,
        provider_free: provider_free_count,
        tier1_free: tier1_free_count,
        hierachy_free: hierachy_free_count,
        type_: classify_asn(asn),
    }
}

fn main() {
    let all_asns = TOPOLOGY.ases_map.keys().copied();
    let all_asns_count = all_asns.clone().count();

    let file = File::create("data.csv").unwrap();
    let mut writter = BufWriter::new(file);

    writter
        .write_all(b"asn,type,provider_free,tier1_free,hierachy_free,total\n")
        .unwrap();

    all_asns
        .take(10000)
        .map(|asn| count_hierachy_free_paths(&TOPOLOGY, asn))
        .for_each(|record| {
            let line = format!(
                "{},{},{},{},{},{}\n",
                record.asn,
                record.type_.to_str(),
                record.provider_free,
                record.tier1_free,
                record.hierachy_free,
                all_asns_count,
            );

            writter.write_all(line.as_bytes()).unwrap();
        });
}
