use log::{debug, info, trace};
use once_cell::sync::Lazy;
use rayon::prelude::*;
use std::sync::mpsc;
use std::thread;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufWriter, Write},
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
    12076, // Microsoft (Azure)
    36351, // IBM
    19604, // IBM Cloud
    15169, // Google
    8075,  // Microsoft (Not azure)
    16509, // Amazon Cloud
];

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
    let mut topo = topo.clone();
    info!("---------- {asn} ----------");

    let providers: HashSet<_> = topo
        .providers_of(asn)
        .unwrap()
        .into_iter()
        .filter(|&x| x != asn)
        .collect();

    let tiers1: HashSet<_> = TIER1_ASNS.into_iter().filter(|&x| x != asn).collect();
    let tiers2: HashSet<_> = TIER2_ASNS.into_iter().filter(|&x| x != asn).collect();

    debug!("Remove providers");
    providers.iter().for_each(|&provider| {
        topo.index_of(provider).map(|provider_idx| {
            topo.graph.remove_node(provider_idx);
        });
    });

    info!(
        "Without providers graph has {} nodes",
        topo.graph.node_count()
    );

    debug!("Transform to paths graph withouth providers");
    let mut topo: Topology = topo.valley_free_of(asn).into();
    let provider_free_count = topo.graph.node_count();
    info!("Provider free graph has {} nodes", provider_free_count);

    debug!("Remove tier1");
    tiers1.iter().for_each(|&tier1| {
        topo.index_of(tier1).map(|tier1_idx| {
            topo.graph.remove_node(tier1_idx);
        });
    });

    debug!("Transform to paths graph withouth tier1 and providers");
    let mut topo: Topology = topo.valley_free_of(asn).into();
    let tier1_free_count = topo.graph.node_count();
    info!("Tier1 free graph has {} nodes", tier1_free_count);

    debug!("Remove tier2");
    tiers2.iter().for_each(|&tier2| {
        topo.index_of(tier2).map(|tier2_idx| {
            topo.graph.remove_node(tier2_idx);
        });
    });
    debug!("Transform to paths graph withouth tier1, tier2 and providers");
    let mut topo: Topology = topo.valley_free_of(asn).into();
    let hierachy_free_count = topo.graph.node_count();
    info!("Hierachy free graph has {} nodes", hierachy_free_count);

    DataRecord {
        asn,
        provider_free: provider_free_count,
        tier1_free: tier1_free_count,
        hierachy_free: hierachy_free_count,
        type_: classify_asn(asn),
    }
}

fn main() {
    env_logger::init();

    let file = std::include_bytes!("../20231201.as-rel2.txt");
    let base_topology = Topology::from_caida(&file[..]).unwrap();

    let all_asns = base_topology.all_asns();
    let all_asns_count = all_asns.len();

    let (tx, rx) = mpsc::channel::<String>();

    thread::spawn(move || {
        let file = File::create("data_2023.csv").unwrap();
        let mut writter = BufWriter::new(file);

        writter
            .write_all(b"asn,type,provider_free,tier1_free,hierachy_free,total\n")
            .unwrap();
        writter.flush().unwrap();

        for buf in rx.iter() {
            writter.write_all(buf.as_bytes()).unwrap();
        }

        writter.flush().unwrap();
    });

    let all_asns = CLOUD_PROVIDERS
        .iter()
        .chain(TIER1_ASNS.iter())
        .chain(TIER2_ASNS.iter())
        .copied()
        .collect::<Vec<_>>();

    all_asns
        .into_par_iter()
        .map(|asn| count_hierachy_free_paths(&base_topology, asn))
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

            tx.send(line).unwrap();
        });
}
