use polars::prelude::*;

fn main() {
    let df = CsvReader::from_path("data.csv")
        .unwrap()
        .infer_schema(None)
        .has_header(true)
        .finish()
        .unwrap();

    let df = df
        .sort(["hierachy_free"], true, false)
        .unwrap()
        .head(Some(20));

    println!("{}", df);
}
