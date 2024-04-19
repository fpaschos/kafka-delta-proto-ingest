use deltalake::DeltaOps;
use deltalake::kernel::{DataType, PrimitiveType};
use deltalake::protocol::SaveMode;
use deltalake::writer::{DeltaWriter, JsonWriter, WriteMode};
use serde_json::json;

/// Example for learning purposes create table
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Open and load the latest table state from table_uri.
    // This function errors if the table does not already exist.
    let mut table = deltalake::open_table("./data/claims").await?;

    let mut writer = JsonWriter::for_table(&table)?;

    let data: Vec<_> = (1..1000).map(|i| {
        json!({
            "id": i,
            "name": format!("name-{}", i),
            "foo": 12,
        })
    }).collect();
    let data_len = data.len();

    writer.write(data).await?;
    writer.flush_and_commit(&mut table).await?;

    println!("Wrote data size {} to table {}",data_len ,table.table_uri());


    Ok(())
}