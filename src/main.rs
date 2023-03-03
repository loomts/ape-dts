use crate::task::task_runner::TaskRunner;
use dotenv::dotenv;
use error::Error;
use std::env;
use task::task_util::TaskUtil;

mod config;
mod error;
mod extractor;
mod meta;
mod sinker;
mod task;
mod test;
mod traits;

const TASK_CONFIG: &str = "TASK_CONFIG";

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let task_config = if args.len() > 1 {
        args[1].clone()
    } else {
        dotenv().ok();
        env::var(TASK_CONFIG).unwrap()
    };

    let task_config = "/Users/xushicai/Documents/projects/ape-dts/src/test/pg_to_pg/snapshot_basic_test/task_config.ini";
    TaskRunner::start_task(&task_config).await.unwrap()

    // test_numeric().await.unwrap();
}

async fn test_json() -> Result<(), Error> {
    let url = "postgres://postgres:postgres@127.0.0.1:5432/postgres2";
    let sql = "INSERT INTO text_table_2(j, jb, u) VALUES ($1::json, $2::jsonb, $3::UUID)";
    let pool = TaskUtil::create_pg_conn_pool(&url, 1, true).await.unwrap();
    let mut query = sqlx::query(sql);

    let a = "{\"bar\": \"baz\"}";
    let b = "{\"bar\": \"baz\"}";
    let c = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";

    query = query.bind(a).bind(b).bind(c);
    query.execute(&pool).await.unwrap();
    Ok(())
}

async fn test_numeric() -> Result<(), Error> {
    // INSERT INTO numeric_table (si, i, bi, r, db,
    //     r_int, db_int, r_nan, db_nan, r_pinf, db_pinf,
    //     r_ninf, db_ninf, ss, bs, b, o)
    //     VALUES (5, 123456, 1234567890123, '3.3'::float4, '4.44'::float8,
    //     3, 4, 'NaN'::float4, 'NaN'::float8, 'Infinity'::float4, 'Infinity'::float8,
    //     '-Infinity'::float4, '-Infinity'::float8, 1, 123, true, 4000000000::oid)

    let url = "postgres://postgres:postgres@127.0.0.1:5432/postgres2";
    let sql = "INSERT INTO numeric_table (si, i, bi, r, db,
        r_int, db_int, r_nan, db_nan, r_pinf, db_pinf,
        r_ninf, db_ninf, ss, bs, b, o)
        VALUES (5, 123456, 1234567890123, $1::float4, $2::float8,
        3, 4, $3::float4, $4::float8, $5::float4, $6::float8,
        $7::float4, $8::float8, 1, 123, true, $9::oid)";
    let pool = TaskUtil::create_pg_conn_pool(&url, 1, true).await.unwrap();
    let mut query = sqlx::query(sql);

    query = query
        .bind("3.3")
        .bind("4.44")
        .bind("NaN")
        .bind("NaN")
        .bind("Infinity")
        .bind("Infinity")
        .bind("-Infinity")
        .bind("-Infinity")
        .bind("4000000000");

    query.execute(&pool).await.unwrap();
    Ok(())
}
