use schema_registry_converter::async_impl::schema_registry::SrSettings;

mod registry;

const SCHEMA_REGISTRY_URL: &str = "http://localhost:58085/";

const TOPIC: &str = "proto.ds.claim";


#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let settings = SrSettings::new(SCHEMA_REGISTRY_URL.to_string());

    let registry = registry::RegistryContext::new(settings);
    let schemas = registry.schemas_of_topic(TOPIC).await?;


    println!("{:?}", schemas);

    // let context = Context::parse(&[r#"
    //   syntax = "proto3";
    //   package model;
    //
    //   import "google/protobuf/timestamp.proto";
    //   import "shared.proto";
    //
    //   message Claim {
    //       optional string assignee = 1;
    //       string claim_no = 2;
    //       UserCompany.Enum company = 3;
    //       string created_by = 4;
    //       google.protobuf.Timestamp created_date = 5;
    //       FraudStatus.Enum fraud_status = 6;
    //       int32 id = 7;
    //   }
    // "#])?;

    // println!("{:?}", context);



    Ok(())
}