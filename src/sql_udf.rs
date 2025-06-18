use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch},
        datatypes::{DataType, Field, Schema},
    },
    config::ConfigOptions,
    error::{DataFusionError, Result},
    execution::{
        SessionState,
        context::{FunctionFactory, RegisterFunction},
    },
    logical_expr::{
        ColumnarValue, CreateFunction, Signature, Volatility,
        async_udf::{AsyncScalarFunctionArgs, AsyncScalarUDFImpl},
    },
    prelude::*,
    scalar::ScalarValue,
};

#[derive(Debug, Clone)]
pub struct SqlUdf {
    signature: Signature,
    name: String,
    parameters: Vec<Field>,
    sql: String,
    return_type: DataType,
}

impl SqlUdf {
    fn new(name: String, parameters: Vec<Field>, sql: String, return_type: DataType) -> Self {
        Self {
            signature: Signature::exact(
                parameters
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect(),
                Volatility::Immutable,
            ),
            name,
            parameters,
            sql,
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
        options: &ConfigOptions,
    ) -> Result<ArrayRef> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let output_length = args[0].len();

        let args_batch = RecordBatch::try_new(Schema::new(self.parameters.clone()).into(), args)?;

        let mut config = SessionConfig::new();

        // TODO is there a nicer way to copy options to the udf config? should we copy all these options?
        let config_options = config.options_mut();
        config_options.catalog = options.catalog.clone();
        config_options.execution = options.execution.clone();
        config_options.optimizer = options.optimizer.clone();
        config_options.sql_parser = options.sql_parser.clone();
        config_options.explain = options.explain.clone();
        config_options.extensions = options.extensions.clone();
        config_options.format = options.format.clone();

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

#[derive(Debug, Clone)]
struct CollectSqlUdfs {
    udfs: Arc<Mutex<Vec<SqlUdf>>>,
}

impl Default for CollectSqlUdfs {
    fn default() -> Self {
        Self {
            udfs: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl FunctionFactory for CollectSqlUdfs {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        let args = statement.args.unwrap();
        let body_expr = statement.params.function_body.unwrap();
        dbg!(&body_expr);
        let Expr::Literal(ScalarValue::Utf8(Some(sql))) = body_expr else {
            return Err(DataFusionError::Internal(
                "Expected a string literal".to_string(),
            ));
        };

        let sql_udf = SqlUdf::new(
            statement.name,
            args.into_iter()
                .map(|a| Field::new(a.name.unwrap().to_string(), a.data_type.clone(), true))
                .collect(),
            sql,
            statement.return_type.unwrap(),
        );
        self.udfs.lock().unwrap().push(sql_udf);

        // this function isn't used, we just need something to return
        let dummy_udf = create_udf(
            "dummy",
            vec![],
            DataType::Null,
            Volatility::Immutable,
            Arc::new(|_: &[ColumnarValue]| Ok(ColumnarValue::Scalar(ScalarValue::Null))),
        );
        Ok(RegisterFunction::Scalar(Arc::new(dummy_udf)))
    }
}

pub async fn parse_functions(sql: &str) -> Result<Vec<SqlUdf>> {
    let factory = Arc::new(CollectSqlUdfs::default());
    let ctx = SessionContext::new().with_function_factory(factory.clone());
    for chunk in sql.split("\n\n") {
        ctx.sql(chunk).await?;
    }
    Ok(factory.udfs.lock().unwrap().clone())
}
