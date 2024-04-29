use schema_registry_converter::async_impl::schema_registry::SrSettings;

mod registry;
mod proto_schema;

const SCHEMA_REGISTRY_URL: &str = "http://localhost:58085/";

const TOPIC: &str = "proto.ds.claim";


#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let settings = SrSettings::new(SCHEMA_REGISTRY_URL.to_string());

    let registry = registry::SchemaRegistryContext::new(settings);
    let schemas = registry.schemas_of_topic(TOPIC).await?;


    println!("{:?}", schemas);




    Ok(())
}