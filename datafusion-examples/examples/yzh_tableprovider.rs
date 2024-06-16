use std::any::Any;
use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::prelude::SessionContext;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{Expr, TableType};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udtf("query_csv", Arc::new(CSVQueryFunc {}));

    let testdata = datafusion::test_util::arrow_test_data();
    let csv_file = format!("{testdata}/csv/aggregate_test_100.csv");

    let df = ctx
        .sql(format!("SELECT * FROM query_csv('{csv_file}', 1+5);").as_str())
        .await?;
    df.show().await?;

    let df = ctx.sql(format!("SELECT * FROM query_csv('{csv_file}');").as_str())
        .await?;
    df.show().await?;

    Ok(())
}

struct CSVQueryFunc {}

impl TableFunctionImpl for CSVQueryFunc {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(ref path)))) = args.first() else {
            return plan_err!("read csv requires at list one string arg");
        };
        let limit = args.get(1)
            .map(|expr| {
                let execution_props = ExecutionProps::new();
                let info = SimplifyContext::new(&execution_props);
                let expr = ExprSimplifier::new(info).simplify(expr.clone())?;
                if let Expr::Literal(ScalarValue::Int64(Some(limit))) = expr {
                    Ok(limit as usize)
                } else {
                    plan_err!("limit must be an integer")
                }
            })
            .transpose()?;
        let (schema, batches) = read_cs_batches_from_local(path)?;
        let table = CSVTableProvider {
            schema,
            limit,
            batches,
        };
        Ok(Arc::new(table))
    }
}

// 试一下不实现 Send 和 Sync 约束会怎么样
struct CSVTableProvider {
    schema: SchemaRef, // arrow 中描述一个schema的对象
    limit: Option<usize>,
    batches: Vec<RecordBatch>, // RecordBatch arrow 中描述二维数据集，每行数据长度相同。这里表示多个二维数据集
}
#[async_trait]
impl TableProvider for CSVTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        return self.schema.clone();
    }

    fn table_type(&self) -> TableType {
        // 这是什么意思
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>, // 描述了一个大table中，只有一小部分column会被扫描到。通过schema字段索引
        _filters: &[Expr],  // 需要实现Self::supports_filters_pushdown方法，否则为空值
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {

        // todo scan 里面的各个参数是干嘛的
        // 如果 self.limit 不为 None值，执行第一个块。否则执行else块

        let batches = if let Some(max_return_lines) = self.limit {
            let mut batches = vec![];
            let mut lines = 0;
            for batch in &self.batches { // 遍历每一个数据集
                let batch_lines = batch.num_rows();
                if lines + batch_lines > max_return_lines {
                    let batch_lines = max_return_lines - lines;
                    batches.push(batch.slice(0, batch_lines));
                    break;
                } else {
                    batches.push(batch.clone());
                    lines += batch_lines;
                }
            }
            batches
        } else {
            self.batches.clone()
        };
        Ok(Arc::new(MemoryExec::try_new(
            &[batches],
            TableProvider::schema(self),
            projection.cloned(),
        )?))
    }
}

fn read_cs_batches_from_local(csv_path: impl AsRef<Path>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut file = File::open(csv_path)?;
    let (schema, _) = Format::default().infer_schema(&mut file, None)?;
    file.rewind()?;

    let reader = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_header(true)
        .build(file)?;
    let mut batches = vec![];
    for batch in reader {
        batches.push(batch?);
    }
    let schema = Arc::new(schema);
    Ok((schema, batches))
}