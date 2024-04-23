use std::sync::Arc;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures_util::future::{BoxFuture, Shared};
use futures_util::FutureExt;
use schema_registry_converter::async_impl::schema_registry::{get_referenced_schema, get_schema_by_id_and_type, get_schema_by_subject, SrSettings};
use schema_registry_converter::schema_registry_common::{RegisteredSchema, SchemaType};
use schema_registry_converter::schema_registry_common::SubjectNameStrategy::TopicNameStrategy;


type SharedFutureSchema<'a> = Shared<BoxFuture<'a, Result<Arc<Vec<String>>, RegistryError>>>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum RegistryError {
    // #[error("Registry error")]
    // RegistryError,

    #[error("Schema registry converter error: {source}")]
    SchemaRegistryError {
        #[from]
        source: schema_registry_converter::error::SRCError
    },
}

pub struct RegistryContext {
    settings: SrSettings,
    schemas: DashMap<u32, Arc<Vec<String>>>,
    cache: DashMap<u32, SharedFutureSchema<'static>>,
}

impl RegistryContext {
    pub fn new(settings: SrSettings) -> Self {
        Self {
            settings,
            schemas: DashMap::new(),
            cache: DashMap::new(),
        }
    }

    pub async fn schemas_of_topic(&self, topic: &str) -> Result<Arc<Vec<String>>, RegistryError> {
        let subject = TopicNameStrategy(topic.into(), false);
        let schema = get_schema_by_subject(&self.settings, &subject).await?;
        if let Some(s) = self.schemas.get(&schema.id) {
            return Ok(s.value().clone());
        } else {
            return self.schemas_of(schema.id).await;
        }
    }

    pub async fn schemas_of(&self, id: u32) -> Result<Arc<Vec<String>>, RegistryError> {
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

    fn get_schemas_by_shared_future(&self, id: u32) -> SharedFutureSchema<'static> {
        match self.cache.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let settings = self.settings.clone();
                let future = async move {
                    let schema = get_schema_by_id_and_type(id, &settings, SchemaType::Protobuf).await;
                    match schema {
                        Ok(schema) => get_all_schema_references(&settings, schema).await,
                        Err(e) => Err(RegistryError::SchemaRegistryError { source: e }),
                    }
                }
                    .boxed()
                    .shared();
                e.insert(future).value().clone()
            }
        }
    }
}

pub async fn get_all_schema_references(
   settings: &SrSettings,
   schema: RegisteredSchema,
) -> Result<Arc<Vec<String>>, RegistryError> {
    let mut res = Vec::new();
    get_all_schemas_recursive(settings, schema, &mut res).await?;
    Ok(Arc::new(res))
}

pub fn get_all_schemas_recursive<'a>(
    settings: &'a SrSettings,
    schema: RegisteredSchema,
    res: &'a mut Vec<String>,
) -> BoxFuture<'a, Result<(), RegistryError>> {
    async move {
        for s in schema.references {
            let schema = get_referenced_schema(settings, &s).await?;
            get_all_schemas_recursive(settings, schema, res).await?;
        }
        res.push(schema.schema);
        Ok(())
    }.boxed()
}

