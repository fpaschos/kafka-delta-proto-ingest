use std::sync::Arc;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use futures_util::future::{BoxFuture, Shared};
use futures_util::{FutureExt};
use schema_registry_converter::async_impl::schema_registry::{get_referenced_schema, get_schema_by_id_and_type, get_schema_by_subject, SrSettings};
use schema_registry_converter::schema_registry_common::{RegisteredSchema, SchemaType};
use schema_registry_converter::schema_registry_common::SubjectNameStrategy::TopicNameStrategy;
use crate::proto_schema::ProtoSchema;


#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaRegistryError
{
    #[error("Schema registry converter error: {source}")]
    InternalSchemaRegistryError {
        #[from]
        source: schema_registry_converter::error::SRCError
    },

    #[error("Schema registry error: {source}")]
    CompileError {
        #[from]
        source: Arc<protofish::context::ParseError>,
    },


    #[error("Arrow schema conversion error: {0}")]
    ArrowSchemaGenerationError(
        String,
    ),

    #[error("Json generation error: {0}")]
    DecodeJsonError(
        String
    ),
}


impl From<protofish::context::ParseError> for SchemaRegistryError {
    fn from(e: protofish::context::ParseError) -> Self {
        SchemaRegistryError::CompileError { source: Arc::new(e) }
    }
}

type SharedFutureSchema = Shared<BoxFuture<'static, Result<Arc<Vec<String>>, SchemaRegistryError>>>;

pub struct SchemaRegistry {
    settings: SrSettings,
    schemas: DashMap<u32, Arc<Vec<String>>>,
    cache: DashMap<u32, SharedFutureSchema>,
    compiled_schemas: DashMap<u32, Arc<ProtoSchema>>,
}

impl SchemaRegistry {
    pub fn new(settings: SrSettings) -> Self {
        Self {
            settings,
            schemas: DashMap::new(),
            cache: DashMap::new(),
            compiled_schemas: DashMap::new(),
        }
    }

    pub async fn schemas_of_topic(&self, topic: &str) -> Result<Arc<Vec<String>>, SchemaRegistryError> {
        let subject = TopicNameStrategy(topic.into(), false);
        let schema = get_schema_by_subject(&self.settings, &subject).await?;
        return if let Some(s) = self.schemas.get(&schema.id) {
            Ok(s.value().clone())
        } else {
            self.schemas_of(schema.id).await
        }
    }

    pub async fn schemas_of(&self, id: u32) -> Result<Arc<Vec<String>>, SchemaRegistryError> {
        let schemas = self.schemas.get(&id);
        if let Some(s) = schemas {
            return Ok(s.value().clone());
        } else {
            let res = self.get_schemas_by_shared_future(id).await;
            if res.is_ok() && !self.schemas.contains_key(&id) {
                self.schemas.insert(id, res.clone().unwrap());
                self.cache.remove(&id);
            }
            res
        }
    }

    pub async fn compiled_schema_of(&self, id: u32) -> Result<ProtoSchema, SchemaRegistryError> {
        let schemas = self.schemas_of(id).await?;
        let compiled = ProtoSchema::try_compile(schemas.as_slice())?;
        Ok(compiled)
    }

    fn get_schemas_by_shared_future(&self, id: u32) -> SharedFutureSchema {
        match self.cache.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let settings = self.settings.clone();
                let future = async move {
                    let schema = get_schema_by_id_and_type(id, &settings, SchemaType::Protobuf).await;
                    match schema {
                        Ok(schema) => get_all_schema_references(&settings, schema).await,
                        Err(e) => Err(SchemaRegistryError::InternalSchemaRegistryError { source: e }),
                    }
                }
                    .boxed()
                    .shared();
                e.insert(future).value().clone()
            }
        }
    }

    /// Insert raw string schemas to a specific id (used only for testing purposes)
    #[cfg(test)]
    pub fn insert_raw_schemas(&self, id: u32, schemas: Vec<String>) -> Result<(), SchemaRegistryError> {
        self.schemas.insert(id, Arc::new(schemas));
        Ok(())
    }
}
async fn get_all_schema_references(
   settings: &SrSettings,
   schema: RegisteredSchema,
) -> Result<Arc<Vec<String>>, SchemaRegistryError> {
    let mut res = Vec::new();
    get_all_schemas_recursive(settings, schema, &mut res).await?;
    Ok(Arc::new(res))
}
fn get_all_schemas_recursive<'a>(
    settings: &'a SrSettings,
    schema: RegisteredSchema,
    res: &'a mut Vec<String>,
) -> BoxFuture<'a, Result<(), SchemaRegistryError>> {
    async move {
        for s in schema.references {
            let schema = get_referenced_schema(settings, &s).await?;
            get_all_schemas_recursive(settings, schema, res).await?;
        }
        res.push(schema.schema);
        Ok(())
    }.boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA_REGISTRY_URL: &str = "http://localhost:58085/";

    fn get_proto_sample() -> &'static str {
        r#"
        syntax = "proto3";
        package model;

        // import "google/protobuf/timestamp.proto";
        // import "shared.proto";

        message Task {
            string id = 1;
            string created_by = 2;
            int64 created_date = 3;
            Status status = 4;
        }

        enum Status {
            UNKNOWN = 0;
            ACTIVE = 1;
            INACTIVE = 2;
        }
        "#
    }

    #[tokio::test]
    pub async fn test() {
        let settings = SrSettings::new(SCHEMA_REGISTRY_URL.to_string());

        let registry = SchemaRegistry::new(settings);
        registry.insert_raw_schemas(80, vec![get_proto_sample().to_string()]).unwrap();

        let res = registry.compiled_schema_of(80).await.unwrap();

        let ctx = &res.context;
        let _info = ctx.get_message("model.Task").unwrap();


        // res.context.

        // println!("{:?}", res);
        // println!("{:?}", info);

        // let arrow_schema = res.to_arrow_schema().unwrap();
        // println!("{:?}", arrow_schema);
    }
}