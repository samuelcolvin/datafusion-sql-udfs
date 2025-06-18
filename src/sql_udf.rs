use std::{
    any::Any,
    fmt::Display,
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
        ColumnarValue, CreateFunction, ScalarUDF, Signature, Volatility,
        async_udf::{AsyncScalarFunctionArgs, AsyncScalarUDFImpl},
    },
    prelude::*,
    scalar::ScalarValue,
    sql::parser::Statement,
};
use regex::RegexBuilder;

#[derive(Debug, Clone)]
pub struct SqlUdf {
    signature: Signature,
    name: String,
    schema: Arc<Schema>,
    body: Statement,
    return_type: DataType,
}

impl Display for SqlUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let args = self
            .schema
            .fields()
            .iter()
            .map(|field| format!("{} {}", field.name(), field.data_type()))
            .collect::<Vec<_>>();
        write!(
            f,
            "{}({}) -> {}",
            self.name,
            args.join(", "),
            self.return_type
        )
    }
}

impl SqlUdf {
    fn new(name: String, parameters: Vec<Field>, body: Statement, return_type: DataType) -> Self {
        Self {
            signature: Signature::exact(
                parameters
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect(),
                Volatility::Immutable,
            ),
            name,
            schema: Arc::new(Schema::new(parameters)),
            body,
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

        let arg_arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let output_length = arg_arrays[0].len();
        let args_batch = RecordBatch::try_new(self.schema.clone(), arg_arrays)?;
        ctx.register_batch("arguments", args_batch)?;

        // let output_batches = ctx.sql(&self.sql).await?.collect().await?;
        let plan = ctx.state().statement_to_plan(self.body.clone()).await?;
        SQLOptions::new().verify_plan(&plan)?;
        let output_batches = ctx.execute_logical_plan(plan).await?.collect().await?;

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
    // this function isn't used, we just need something to return from `create`
    dummy_udf: Arc<ScalarUDF>,
}

impl Default for CollectSqlUdfs {
    fn default() -> Self {
        Self {
            udfs: Arc::new(Mutex::new(Vec::new())),
            dummy_udf: Arc::new(create_udf(
                "dummy",
                vec![],
                DataType::Null,
                Volatility::Immutable,
                Arc::new(|_: &[ColumnarValue]| Ok(ColumnarValue::Scalar(ScalarValue::Null))),
            )),
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
        let Expr::Literal(ScalarValue::Utf8(Some(sql))) = body_expr else {
            return Err(DataFusionError::Plan(
                "Expected a string literal for the function body".to_string(),
            ));
        };

        let config = SessionConfig::new();
        let dialect = config.options().sql_parser.dialect.clone();
        let ctx = SessionContext::new_with_config(config);
        let body = ctx.state().sql_to_statement(&sql, &dialect)?;

        let sql_udf = SqlUdf::new(
            statement.name,
            args.into_iter()
                .map(|a| Field::new(a.name.unwrap().to_string(), a.data_type.clone(), true))
                .collect(),
            body,
            statement.return_type.unwrap(),
        );
        self.udfs.lock().unwrap().push(sql_udf);

        Ok(RegisterFunction::Scalar(self.dummy_udf.clone()))
    }
}

pub async fn parse_functions(sql: &str) -> Result<Vec<SqlUdf>> {
    let factory = Arc::new(CollectSqlUdfs::default());
    let ctx = SessionContext::new().with_function_factory(factory.clone());
    let re = RegexBuilder::new(r"^\s*CREATE\s+FUNCTION.+?LANGUAGE\s+SQL\s*;\s*$")
        .case_insensitive(true)
        .multi_line(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap();

    for capture in re.captures_iter(sql) {
        let chunk = capture.get(0).unwrap().as_str();
        ctx.sql(chunk).await?;
    }
    Ok(factory.udfs.lock().unwrap().clone())
}
