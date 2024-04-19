use deltalake::DeltaOps;
use deltalake::kernel::{DataType, PrimitiveType};
use deltalake::protocol::SaveMode;

/// Example for learning purposes create table
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Recreate table
    let table = DeltaOps::try_from_uri("./data/claims")
        .await?
        .create()
        .with_table_name("claim")
        .with_comment("Claims table")
        .with_save_mode(SaveMode::Overwrite)
        .with_column("id", DataType::Primitive(PrimitiveType::Integer), false, None)
        .await?;


    println!("Table created: {}", table);
    println!("Table schema: {:?}", table.get_schema());

    Ok(())
}