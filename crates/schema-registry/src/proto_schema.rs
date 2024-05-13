use std::collections::HashSet;

use deltalake::arrow::datatypes::Schema as ArrowSchema;
use protofish::context::Context;
use serde_json::Value as JsonValue;

use crate::arrow::to_arrow_schema;
use crate::json::decode_message_to_json;
use crate::proto_common_types::add_common_files;
use crate::proto_resolver::ProtoResolver;
use crate::registry::SchemaRegistryError;

#[derive(Debug)]
pub struct ProtoSchema {
    pub context: Context,
    pub full_name: String,

}

impl ProtoSchema {
    pub fn try_compile(raw_schemas: &[String]) -> Result<Self, SchemaRegistryError> {
        // TODO find top level full name from last schema
        Self::try_compile_with_full_name("".to_string(), raw_schemas)
    }

    pub fn try_compile_with_full_name<S: AsRef<str>>(full_name: S, raw_schemas: &[String]) -> Result<Self, SchemaRegistryError> {
        let mut schemas = Vec::new();
        for s in raw_schemas {
            let schema_info = ProtoResolver::resolve(s)?;
            add_common_files(schema_info.imports(), &mut schemas);
            schemas.push(s.to_string());
        }

        let unique_schemas: HashSet<String> = schemas.into_iter().collect();


        let context = Context::parse(unique_schemas)?;
        Ok(Self {
            context,
            full_name: full_name.as_ref().to_string(),
        })
    }


    #[inline]
    pub fn full_name(&self) -> &str {
        &self.full_name
    }

    #[inline]
    pub fn context(&self) -> &Context {
        &self.context
    }

    pub fn to_arrow_schema(&self) -> Result<ArrowSchema, SchemaRegistryError> {
        let info = self.context.get_message(&self.full_name)
            .ok_or(SchemaRegistryError::ArrowSchemaGenerationError(format!("Proto message definition not found {:?}", self.full_name)))?;
        let schema = to_arrow_schema(&self.context, info)?;
        Ok(schema)
    }

    pub fn decode_to_json(&self, data: &[u8]) -> Result<JsonValue, SchemaRegistryError> {
        let info = self.context.get_message(&self.full_name)
            .ok_or(SchemaRegistryError::DecodeJsonError(format!("Proto message definition not found {:?}", self.full_name)))?;

        let value = self.context.decode(info.self_ref, data);
        decode_message_to_json(&self.context, &info, value)
    }
}


#[cfg(test)]
pub mod tests {
    use super::*;

    // Simple schema definition (used in multiple unit tests see: arrow, json modules)
    pub fn simple_schema_sample() -> Vec<String> {
        vec![
            r#"
            syntax = "proto3";
            package example;
            message Person {
                int32 id = 1;
                string name = 2;
                Status status = 4;
                WrappedStatus.Enum wrapped_status = 5;
                Details details = 6;
                repeated Contact contacts = 7;
                repeated WrappedStatus.Enum wrapped_statuses = 8;
                repeated int32 ids = 9;
                repeated Status statuses = 10;
            }

            enum Status  {
                UNKNOWN = 0;
                ACTIVE = 1;
                INACTIVE = 2;
            }

            message WrappedStatus {
                enum Enum {
                    UNKNOWN = 0;
                    ACTIVE = 1;
                    INACTIVE = 2;
                }
            }

            message Contact {
                string address = 1;
                string phone = 2;
                string email = 3;
            }

            message Details {
               uint32 age = 1;
               uint64 salary = 2;
            }
            "#.to_string(),
        ]
    }

    // Complex schema definition, nested and split in multiple strings (used in multiple unit tests see: arrow, json modules)
    pub fn complex_schema() -> Vec<String> {
        vec![
            // shared.proto
            r#"
            syntax = "proto3";

            package example;

            message Status {
                enum Enum {
                    UNKNOWN = 0;
                    ACTIVE = 1;
                    INACTIVE = 2;
                }
            }

            message Contact {
                string address = 1;
                string phone = 2;
                string email = 3;
            }
            "#.to_string(),

            // person.proto
            r#"
            syntax = "proto3";

            package example;

            import "google/protobuf/timestamp.proto";
            import "shared.proto";


            message Person {
                int32 id = 1;
                string name = 2;
                Status.Enum status = 3;
                repeated Contact contacts = 4;

                google.protobuf.Timestamp created_date = 5;
                string created_by = 6;
            }
            "#.to_string(),
        ]
    }

    // Nested polymorphic schema definition, nested and split in multiple strings (used in multiple unit tests see: arrow, json modules)
   pub fn nested_polymorphic_schema() -> Vec<String> {
        vec![
            // shared.proto
            r#"
            syntax = "proto3";

            package example;

            message Status {
                enum Enum {
                    UNKNOWN = 0;
                    ACTIVE = 1;
                    INACTIVE = 2;
                }
            }

            message Contact {
                string address = 1;
                string phone = 2;
                string email = 3;
            }
            "#.to_string(),

            // details.proto
            r#"
            syntax = "proto3";
            package example.details;

            import "google/protobuf/timestamp.proto";


            message DetailsType {
                enum Enum {
                    UNKNOWN = 0;
                    PHYSICAL = 1;
                    FINANCIAL = 2;
                }
            }


            message Details {
                oneof data {
                    Physical physical = 1;
                    Financial financial = 2;
                }
            }

            message Physical {
                DetailsType.Enum type = 1;
                uint32 age = 2;
                google.protobuf.Timestamp created_date = 3;
                string created_by = 4;
            }

            message Financial {
                DetailsType.Enum type = 1;
                uint64 salary = 2;
                google.protobuf.Timestamp created_date = 3;
                string created_by = 4;
            }
            "#.to_string(),

            // person.proto
            r#"
            syntax = "proto3";

            package example;

            import "shared.proto";
            import "details.proto";

            message Person {
                int32 id = 1;
                string name = 2;
                Status.Enum status = 3;
                repeated Contact contacts = 4;
                example.details.Details details = 5;
            }
            "#.to_string(),
        ]
    }

    #[test]
    fn compile_simple_schema() {
        let raw_schemas = simple_schema_sample();
        let proto_schema = ProtoSchema::try_compile(&raw_schemas);
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        assert_eq!(proto_schema.full_name, "".to_string());
    }

    #[test]
    fn compile_complex_schema() {
        let raw_schemas = complex_schema();
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person", &raw_schemas);
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        assert_eq!(&proto_schema.full_name, "example.Person");
    }

    #[test]
    fn compile_nested_polymorphic_schema() {
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person", nested_polymorphic_schema().as_slice());
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        assert_eq!(&proto_schema.full_name, "example.Person");
    }
}