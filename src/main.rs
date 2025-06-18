use std::{sync::Arc, time::Instant};

use datafusion::{error::Result, logical_expr::async_udf::AsyncScalarUDF, prelude::*};

mod sql_udf;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let content = std::fs::read_to_string("udfs.sql").unwrap();
    let udfs = sql_udf::parse_functions(&content).await?;

    println!(
        "Found {} functions:\n  {}\n",
        udfs.len(),
        udfs.iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    for sql_udf in udfs {
        let async_udf = AsyncScalarUDF::new(Arc::new(sql_udf));
        ctx.register_udf(async_udf.into_scalar_udf());
    }

    ctx.register_csv("example", "data.csv", CsvReadOptions::default())
        .await?;

    //////////////////////////////////////////////
    // with direct sql
    //////////////////////////////////////////////
    // warmup
    let df = ctx.sql("SELECT a, a + 1 FROM example").await?;
    df.count().await.unwrap();

    // run
    let start = Instant::now();
    let df = ctx.sql("SELECT a, a + 1 FROM example").await?;
    let elapsed_direct = start.elapsed();

    // df.show().await?;
    dbg!(df.count().await.unwrap());
    println!("Elapsed time (direct): {:?}", elapsed_direct);

    //////////////////////////////////////////////
    // with SQL UDF
    //////////////////////////////////////////////
    // warmup
    let df = ctx.sql("SELECT a, add_one(a) FROM example").await?;
    df.count().await.unwrap();

    let start = Instant::now();
    let df = ctx.sql("SELECT a, add_one(a) FROM example").await?;
    let elapsed_sql_udf = start.elapsed();

    // df.show().await?;
    dbg!(df.count().await.unwrap());
    println!("Elapsed time (SQL UDF): {:?}", elapsed_sql_udf);

    let df = ctx.sql("SELECT llm_cost('gpt-4o', 1, 1)").await?;
    df.show().await?;
    Ok(())
}
