use deltalake::DeltaOps;
use deltalake::kernel::{DataType, PrimitiveType, StructField, StructType};
use deltalake::protocol::SaveMode;
use protofish::context::TypeInfo;
use protofish::decode::EnumValue;
use protofish::prelude::{FieldValue, MessageValue, Value};

use schema_registry::ProtoSchema;
use schema_registry::SchemaRegistryError;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let schema = ProtoSchema::try_compile_with_full_name("example.Person", vec![raw_proto_schema()].as_ref())?;
    let arrow_schema = schema.to_arrow_schema()?;
    let delta_schema: StructType = StructType::try_from(&arrow_schema)?;

    // Uncomment to print the delta schema as json
    // let schema_json= serde_json::to_string(&delta_schema)?;
    // println!("Schema JSON: {}", schema_json);


    // Create persons table (using overwrite mode for simplicity)
    let t = DeltaOps::try_from_uri("./data/persons")
        .await?
        .create()
        .with_table_name("person")
        .with_comment("Persons table")
        .with_save_mode(SaveMode::Overwrite)
        .with_columns(delta_schema.fields().to_vec())
        .await?;

    let persons = (0..100).map(|_| {
        let person = create_random_person_proto_value(&schema);
        schema.decode_to_json(&person)
    }).collect::<Result<Vec<_>,_>>()?;

    // Write persons to the table

    // Read values and display







    Ok(())
}

// Raw protobuf 3 schema
fn raw_proto_schema() -> String {
        r#"
        syntax = "proto3";

        package example;
        import "google/protobuf/timestamp.proto";

        message Person {
            int32 id = 1;
            string name = 2;
            example.Status.Enum status = 3;
            repeated example.Contact contacts = 4;
            example.Details details = 5;
        }


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
            string created_by = 4;
        }

        message Financial {
            DetailsType.Enum type = 1;
            uint64 salary = 2;
            string created_by = 4;
        }

        "#.to_string()
}

fn create_random_person_proto_value(schema: &ProtoSchema) -> Vec<u8> {
    let msg_physical = schema.context.get_message("example.Physical").unwrap();
    let TypeInfo::Enum(details_type) = schema.context.get_type("example.DetailsType.Enum").unwrap()
        else { panic!("Expected enum DetailsType type info") };
    let TypeInfo::Message(timestamp) = schema.context.get_type("google.protobuf.Timestamp").unwrap()
        else { panic!("Expected message Timestamp type info") };
    let physical_value = MessageValue {
        msg_ref: msg_physical.self_ref.clone(),
        garbage: None,
        fields: vec![
            FieldValue {
                number: 1,
                value: Value::Enum(EnumValue {
                    enum_ref: details_type.self_ref.clone(),
                    value: 1,
                }),
            },
            FieldValue {
                number: 2,
                value: Value::UInt32(30),
            },
            // FieldValue {
            //     number: 3,
            //     value: Value::Message(Box::new(MessageValue {
            //         msg_ref: timestamp.self_ref.clone(),
            //         garbage: None,
            //         fields: vec![
            //             FieldValue {
            //                 number: 1,
            //                 value: Value::Int64(1715276726),
            //             },
            //             FieldValue {
            //                 number: 2,
            //                 value: Value::Int32(99_000_000), // 99 milliseconds
            //             },
            //         ]
            //     })),
            // },
            FieldValue {
                number: 4,
                value: Value::String("123e4567-e89b-12d3-a456-426614174000".to_string()),
            }
        ]
    };

    // Construct person message value
    let msg = schema.context.get_message("example.Person").unwrap();
    let msg_detail = schema.context.get_message("example.Details").unwrap();
    let TypeInfo::Enum(status) = schema.context.get_type("example.Status.Enum").unwrap() else { panic!("Expected enum Status type info") };

    let proto_value = MessageValue {
        msg_ref: msg.self_ref.clone(),
        garbage: None,
        fields: vec![
            FieldValue {
                number: 1,
                value: Value::Int32(1),
            },
            FieldValue {
                number: 2,
                value: Value::String("John".to_string()),
            },
            FieldValue {
                number: 3,
                value: Value::Enum(EnumValue {
                    enum_ref: status.self_ref.clone(),
                    value: 1,
                }),
            },
            FieldValue {
                number: 4,
                value: Value::Message(Box::new(MessageValue {
                    msg_ref: schema.context.get_message("example.Contact").unwrap().self_ref.clone(),
                    garbage: None,
                    fields: vec![
                        FieldValue {
                            number: 1,
                            value: Value::String("123 Main St".into()),
                        },
                        FieldValue {
                            number: 2,
                            value: Value::String("555-555-5555".into()),
                        },
                        FieldValue {
                            number: 3,
                            value: Value::String("test@test.com".into()),
                        }
                    ]
                })),

            },
            FieldValue {
                number: 5,
                value: Value::Message(Box::new(MessageValue {
                    msg_ref: msg_detail.self_ref.clone(),
                    garbage: None,
                    fields: vec![
                        FieldValue {
                            number: 1,
                            value: Value::Message(Box::new(physical_value)),
                        }
                    ]
                })),
            }
        ]
    };
    proto_value.encode(&schema.context()).to_vec()
}
