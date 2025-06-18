use std::{any::Any, sync::Arc, time::Instant};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch},
        datatypes::{DataType, Field, Schema},
    },
    config::ConfigOptions,
    error::{DataFusionError, Result},
    logical_expr::{
        ColumnarValue, Signature, Volatility,
        async_udf::{AsyncScalarFunctionArgs, AsyncScalarUDF, AsyncScalarUDFImpl},
    },
    prelude::*,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
struct SqlUdf {
    signature: Signature,
    name: String,
    parameters: Vec<Field>,
    sql: String,
    return_type: DataType,
}

impl SqlUdf {
    fn new(name: &str, sql: &str, parameters: Vec<Field>, return_type: DataType) -> Self {
        Self {
            signature: Signature::exact(
                parameters
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect(),
                Volatility::Immutable,
            ),
            name: name.to_string(),
            parameters,
            sql: sql.to_string(),
            return_type,
        }
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for SqlUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn ideal_batch_size(&self) -> Option<usize> {
        Some(8000)
    }

    async fn invoke_async_with_args(
        &self,
        args: AsyncScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ArrayRef> {
        dbg!(&self.name);
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let output_length = args[0].len();

        let args_batch = RecordBatch::try_new(Schema::new(self.parameters.clone()).into(), args)?;

        let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
        let ctx = SessionContext::new_with_config(config);

        ctx.register_batch("arguments", args_batch)?;

        let output_batches = ctx.sql(&self.sql).await?.collect().await?;

        if output_batches.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "SQL UDF Error: expected 1 output batch, got {}",
                output_batches.len()
            )));
        }
        let first_batch = output_batches.first().unwrap();
        if first_batch.num_columns() != 1 {
            return Err(DataFusionError::Execution(format!(
                "SQL UDF Error: expected 1 output column, got {}",
                first_batch.num_columns()
            )));
        }
        let array = first_batch.column(0).clone();
        if array.len() != output_length {
            return Err(DataFusionError::Execution(format!(
                "SQL UDF Error: output length mismatch, expected {} but got {}",
                output_length,
                array.len()
            )));
        }
        Ok(array)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FieldDefinition {
    name: String,
    data_type: DataType,
}

impl From<FieldDefinition> for Field {
    fn from(field_def: FieldDefinition) -> Self {
        Field::new(&field_def.name, field_def.data_type, true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SqlUdfDefinition {
    name: String,
    sql: String,
    parameters: Vec<FieldDefinition>,
    return_type: DataType,
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let content = std::fs::read_to_string("udfs.json").unwrap();
    let udf_definitions: Vec<SqlUdfDefinition> = serde_json::from_str(&content).unwrap();

    // Register SQL UDFs
    for udf_def in udf_definitions {
        let fields = udf_def.parameters;
        let return_type = udf_def.return_type;

        let sql_udf = SqlUdf::new(
            &udf_def.name,
            &udf_def.sql,
            fields.into_iter().map(Into::into).collect(),
            return_type,
        );
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
    Ok(())
}
