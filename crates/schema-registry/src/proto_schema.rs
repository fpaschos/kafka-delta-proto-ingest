use std::collections::HashSet;

use deltalake::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use protofish::context::{Context, MessageField, MessageInfo, Multiplicity, ValueType};
use protofish::decode::{MessageValue, PackedArray};
use protofish::prelude::{FieldValue, Value};
use serde_json::{json, to_value, Value as JsonValue};

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

pub(crate) fn to_arrow_schema(ctx: &Context, info: &MessageInfo) -> Result<ArrowSchema, SchemaRegistryError> {
    let mut fields = vec![];
    for field in info.iter_fields() {
        let field = message_field_to_arrow(ctx, field)?;
        fields.push(field);
    }
    Ok(ArrowSchema::new(fields))
}

pub(crate) fn message_field_to_arrow(ctx: &Context, info: &MessageField) -> Result<ArrowField, SchemaRegistryError> {
    let is_repeated = matches!(info.multiplicity, Multiplicity::Repeated | Multiplicity::RepeatedPacked);
    let field_type: DataType = match info.field_type {
        ValueType::Double => {
            DataType::Float64
        }
        ValueType::Float => {
            DataType::Float32
        }
        ValueType::Int32 => {
            DataType::Int32
        }
        ValueType::Int64 => {
            DataType::Int64
        }
        ValueType::UInt32 => {
            DataType::UInt32
        }
        ValueType::UInt64 => {
            DataType::UInt64
        }
        ValueType::SInt32 => {
            DataType::Int32
        }
        ValueType::SInt64 => {
            DataType::Int64
        }
        ValueType::Fixed32 => {
            DataType::Int32
        }
        ValueType::Fixed64 => {
            DataType::Int64
        }
        ValueType::SFixed32 => {
            DataType::Int32
        }
        ValueType::SFixed64 => {
            DataType::Int64
        }
        ValueType::Bool => {
            DataType::Boolean
        }
        ValueType::String => {
            DataType::Utf8
        }
        ValueType::Bytes => {
            DataType::Binary
        }
        ValueType::Enum(_) => {
            DataType::Utf8
        }
        ValueType::Message(info) => {
            let info = ctx.resolve_message(info);

            if let Some(ty) = try_map_as_well_known_type(&info) {
                ty
            } else {
                let mut fields = vec![];
                for f in info.iter_fields() {
                    let field = message_field_to_arrow(ctx, f)?;
                    fields.push(field);
                }
                DataType::Struct(fields.into())
            }
        }
    };

    if is_repeated {
        Ok(
            ArrowField::new(info.name.to_owned(),
                            DataType::List(
                                ArrowField::new("element", field_type, false).into()),
                            true)
        )
    } else {
        Ok(ArrowField::new(info.name.to_owned(), field_type, true))
    }
}

/// Maps google well known types to Arrow data types.
/// Returns None if the message is not a well known type.
pub(crate) fn try_map_as_well_known_type(info: &MessageInfo) -> Option<DataType> {
    match info.full_name.as_str() {
        "google.protobuf.Timestamp" => Some(DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Millisecond, None)),
        _ => None
    }
}

pub(crate) fn decode_message_to_json(ctx: &Context, info: &MessageInfo, value: MessageValue) -> Result<JsonValue, SchemaRegistryError> {
    let mut json = json!({});
    for field_value in value.fields {
        let json = json.as_object_mut().expect("Should be always json object");

        if let Some(field_info) = info.get_field(field_value.number) {
            let decoded = decode_field_to_json(ctx, field_value, &info.full_name)?;

            // Handle repeated fields
            if field_info.multiplicity == Multiplicity::Repeated {
                if let Some(JsonValue::Array(values)) = json.get_mut(&field_info.name) {
                    values.push(decoded);
                } else {
                    // An array of values does not exist create a new one and append the new value
                    let new_array = JsonValue::Array(vec![decoded]);
                    json.insert(field_info.name.clone(), new_array);
                }
            } else if field_info.multiplicity == Multiplicity::RepeatedPacked {
                json.insert(field_info.name.clone(), decoded);
            } else {
                // Single or Optional fields
                json.insert(field_info.name.clone(), decoded);
            }
        } else {
            return Err(SchemaRegistryError::DecodeJsonError(format!("Missing field number {} in {} proto message definition.", field_value.number, info.full_name)));
        }
    }


    Ok(json)
}

// TODO maybe return error here Result<Option<...>>
pub(crate) fn try_decode_json_as_well_known_type(_ctx: &Context, info: &MessageInfo, value: &MessageValue) -> Option<JsonValue> {
    match info.full_name.as_str() {
        // Timestamps as Number(i64) in milliseconds
        "google.protobuf.Timestamp" => {
            let seconds = if let Some(seconds) = value.fields.get(0) {
                if let Value::Int64(v) = seconds.value {
                    v
                } else {
                    return None;
                }
            } else {
                return None;
            };

            let nanos = if let Some(nanos) = value.fields.get(1) {
                if let Value::Int32(v) = nanos.value {
                    v
                } else {
                    return None;
                }
            } else {
                return None;
            };

            let millis = seconds * 1000 + nanos as i64 / 1_000_000;
            Some(JsonValue::Number(millis.into()))
        }
        _ => None
    }
}

pub(crate) fn decode_field_to_json(ctx: &Context, field: FieldValue, _parent_full_name: &str) -> Result<JsonValue, SchemaRegistryError> {
    match field.value {
        Value::Bool(v) => Ok(JsonValue::Bool(v)),
        Value::Int32(v) => Ok(JsonValue::Number(v.into())),
        Value::Int64(v) => Ok(JsonValue::Number(v.into())),
        Value::UInt32(v) => Ok(JsonValue::Number(v.into())),
        Value::UInt64(v) => Ok(JsonValue::Number(v.into())),
        Value::Float(v) => to_value(v).map_err(|e| SchemaRegistryError::DecodeJsonError(format!("Error converting float to json: {}", e))),
        Value::Double(v) => to_value(v).map_err(|e| SchemaRegistryError::DecodeJsonError(format!("Error converting double to json: {}", e))),

        Value::SInt32(v) => Ok(JsonValue::Number(v.into())),
        Value::SInt64(v) => Ok(JsonValue::Number(v.into())),
        Value::Fixed32(v) => Ok(JsonValue::Number(v.into())),
        Value::Fixed64(v) => Ok(JsonValue::Number(v.into())),
        Value::SFixed32(v) => Ok(JsonValue::Number(v.into())),
        Value::SFixed64(v) => Ok(JsonValue::Number(v.into())),
        Value::String(v) => Ok(JsonValue::String(v)),
        Value::Bytes(_) => Err(SchemaRegistryError::DecodeJsonError("Bytes field not supported".to_string())),

        Value::Enum(v) => {
            let enum_info = ctx.resolve_enum(v.enum_ref);
            let enum_value = enum_info.get_field_by_value(v.value)
                .ok_or(SchemaRegistryError::DecodeJsonError("Enum value not found".to_string()))?
                .name
                .clone();
            Ok(JsonValue::String(enum_value))
        }
        Value::Message(v) => {
            let info = ctx.resolve_message(v.msg_ref);

            if let Some(well_known_type) = try_decode_json_as_well_known_type(ctx, &info, &v) {
                Ok(well_known_type)
            } else {
                decode_message_to_json(ctx, &info, *v)
            }
        }
        Value::Packed(packed_array) => {
            match packed_array {
                PackedArray::Double(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::Float(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::Int32(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::Int64(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::UInt32(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::UInt64(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::SInt32(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::SInt64(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::Fixed32(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::Fixed64(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::SFixed32(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::SFixed64(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
                PackedArray::Bool(v) => {
                    let vs: Vec<JsonValue> = v.into_iter().map(|v| v.into()).collect();
                    Ok(JsonValue::Array(vs))
                }
            }
        }

        Value::Incomplete(_, _) => Err(SchemaRegistryError::DecodeJsonError("Incomplete field not supported".to_string())),
        Value::Unknown(_) => Err(SchemaRegistryError::DecodeJsonError("Unknown field not supported".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use deltalake::arrow::datatypes::TimeUnit;
    use protofish::context::TypeInfo;
    use protofish::decode::EnumValue;
    use protofish::prelude::{FieldValue, MessageValue, Value};
    use serde_json::json;
    use serde_json::Value as JsonValue;

    use super::*;

    fn simple_schema_sample() -> Vec<String> {
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

    #[test]
    fn compile_simple_schema() {
        let raw_schemas = simple_schema_sample();
        let proto_schema = ProtoSchema::try_compile(&raw_schemas);
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        assert_eq!(proto_schema.full_name, "".to_string());
    }

    #[test]
    fn simple_schema_to_arrow() {
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person", simple_schema_sample().as_slice());
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        let arrow_schema = proto_schema.to_arrow_schema().expect("Can generate arrow schema from proto schema");

        let f = arrow_schema.field(0);
        assert_eq!(f.name(), "id");
        assert_eq!(f.data_type(), &DataType::Int32);
        assert!(f.is_nullable());

        let f = arrow_schema.field(1);
        assert_eq!(f.name(), "name");
        assert_eq!(f.data_type(), &DataType::Utf8);


        let f = arrow_schema.field(2);
        assert_eq!(f.name(), "status");
        assert_eq!(f.data_type(), &DataType::Utf8);

        let f = arrow_schema.field(3);
        assert_eq!(f.name(), "wrapped_status");
        assert_eq!(f.data_type(), &DataType::Utf8);

        let f = arrow_schema.field(4);
        assert_eq!(f.name(), "details");
        assert_eq!(f.data_type(), &DataType::Struct(vec![
            ArrowField::new("age".to_string(), DataType::UInt32, true),
            ArrowField::new("salary".to_string(), DataType::UInt64, true),
        ].into()));

        let f = arrow_schema.field(5);
        assert_eq!(f.name(), "contacts");
        assert_eq!(f.data_type(), &DataType::List(ArrowField::new("element".to_string(),
                                                                  DataType::Struct(vec![
                                                                      ArrowField::new("address".to_string(), DataType::Utf8, true),
                                                                      ArrowField::new("phone".to_string(), DataType::Utf8, true),
                                                                      ArrowField::new("email".to_string(), DataType::Utf8, true), ].into()
                                                                  ), false).into()));

        let f = arrow_schema.field(6);
        assert_eq!(f.name(), "wrapped_statuses");
        assert_eq!(f.data_type(), &DataType::List(ArrowField::new("element".to_string(), DataType::Utf8, false).into()));

        let f = arrow_schema.field(7);
        assert_eq!(f.name(), "ids");
        assert_eq!(f.data_type(), &DataType::List(ArrowField::new("element".to_string(), DataType::Int32, false).into()));
    }

    #[test]
    fn simple_schema_message_to_json() {
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person".to_string(), simple_schema_sample().as_slice());
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");

        let expected_json = json!({
                "id": 1,
                "name": "John",
                "status": "ACTIVE",
                "wrapped_status": "ACTIVE",
                "details": {
                    "age": 30,
                    "salary": 100000,
                },
                "contacts": [
                    {
                        "address": "123 Main St",
                        "phone": "555-555-5555",
                        "email": "test@test.com"
                    },
                    {
                        "address": "456 Elm St",
                        "phone": "555-555-5555",
                        "email": "test@test.com"
                    }
                ],
                "wrapped_statuses": ["ACTIVE", "INACTIVE"],
                "ids": [1, 2, 3]
            });

        let msg = proto_schema.context.get_message("example.Person").unwrap();
        let msg_detail = proto_schema.context.get_message("example.Details").unwrap();
        let msg_contact = proto_schema.context.get_message("example.Contact").unwrap();
        let TypeInfo::Enum(wrapped_status) = proto_schema.context.get_type("example.WrappedStatus.Enum").unwrap() else { panic!("Expected enum WrappedStatus type info") };

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
                    number: 4,
                    value: Value::Int32(1),
                },
                FieldValue {
                    number: 5,
                    value: Value::Int32(1),
                },
                FieldValue {
                    number: 6,
                    value: Value::Message(Box::new(MessageValue {
                        msg_ref: msg_detail.self_ref.clone(),
                        garbage: None,
                        fields: vec![
                            FieldValue {
                                number: 1,
                                value: Value::UInt32(30),
                            },
                            FieldValue {
                                number: 2,
                                value: Value::UInt64(100000),
                            },
                        ],
                    })),
                },
                FieldValue {
                    number: 7,
                    value: Value::Message(Box::new(MessageValue {
                        msg_ref: msg_contact.self_ref.clone(),
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
                            },
                        ],
                    })),
                },
                FieldValue {
                    number: 7,
                    value: Value::Message(Box::new(MessageValue {
                        msg_ref: msg_contact.self_ref.clone(),
                        garbage: None,
                        fields: vec![
                            FieldValue {
                                number: 1,
                                value: Value::String("456 Elm St".into()),
                            },
                            FieldValue {
                                number: 2,
                                value: Value::String("555-555-5555".into()),
                            },
                            FieldValue {
                                number: 3,
                                value: Value::String("test@test.com".into()),
                            },
                        ],
                    })),
                },
                FieldValue {
                    number: 8,
                    value: Value::Enum(EnumValue {
                        enum_ref: wrapped_status.self_ref.clone(),
                        value: 1,
                    }),
                },
                FieldValue {
                    number: 8,
                    value: Value::Enum(EnumValue {
                        enum_ref: wrapped_status.self_ref.clone(),
                        value: 2,
                    }),
                },
                FieldValue {
                    number: 9,
                    value: Value::Packed(PackedArray::Int32(vec![1, 2, 3])),
                },
            ],
        };
        let proto_value = proto_value.encode(&proto_schema.context());

        let json = proto_schema.decode_to_json(proto_value.as_ref()).unwrap();
        assert_eq!(json, expected_json);
    }

    fn complex_schema() -> Vec<String> {
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

    #[test]
    fn compile_complex_schema() {
        let raw_schemas = complex_schema();
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person", &raw_schemas);
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        assert_eq!(&proto_schema.full_name, "example.Person");
    }

    #[test]
    fn complex_schema_to_arrow() {
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person", complex_schema().as_slice());
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        let arrow_schema = proto_schema.to_arrow_schema().expect("Can generate arrow schema from proto schema");

        let f = arrow_schema.field(0);
        assert_eq!(f.name(), "id");
        assert_eq!(f.data_type(), &DataType::Int32);
        assert!(f.is_nullable());

        let f = arrow_schema.field(1);
        assert_eq!(f.name(), "name");
        assert_eq!(f.data_type(), &DataType::Utf8);

        let f = arrow_schema.field(2);
        assert_eq!(f.name(), "status");
        assert_eq!(f.data_type(), &DataType::Utf8);

        let f = arrow_schema.field(3);
        assert_eq!(f.name(), "contacts");
        assert_eq!(f.data_type(), &DataType::List(ArrowField::new("element".to_string(),
                                                                  DataType::Struct(vec![
                                                                      ArrowField::new("address".to_string(), DataType::Utf8, true),
                                                                      ArrowField::new("phone".to_string(), DataType::Utf8, true),
                                                                      ArrowField::new("email".to_string(), DataType::Utf8, true), ].into()
                                                                  ), false).into()));

        let f = arrow_schema.field(4);
        assert_eq!(f.name(), "created_date");
        assert_eq!(f.data_type(), &DataType::Timestamp(TimeUnit::Millisecond, None));

        let f = arrow_schema.field(5);
        assert_eq!(f.name(), "created_by");
        assert_eq!(f.data_type(), &DataType::Utf8);
    }

    fn nested_polymorphic_schema() -> Vec<String> {
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
    fn compile_nested_polymorphic_schema() {
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person", nested_polymorphic_schema().as_slice());
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        assert_eq!(&proto_schema.full_name, "example.Person");
    }

    #[test]
    fn nested_polymorphic_schema_to_arrow() {
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person", nested_polymorphic_schema().as_slice());
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        let arrow_schema = proto_schema.to_arrow_schema().expect("Can generate arrow schema from proto schema");

        let f = arrow_schema.field(0);
        assert_eq!(f.name(), "id");
        assert_eq!(f.data_type(), &DataType::Int32);
        assert!(f.is_nullable());

        let f = arrow_schema.field(1);
        assert_eq!(f.name(), "name");
        assert_eq!(f.data_type(), &DataType::Utf8);

        let f = arrow_schema.field(2);
        assert_eq!(f.name(), "status");
        assert_eq!(f.data_type(), &DataType::Utf8);

        let f = arrow_schema.field(3);
        assert_eq!(f.name(), "contacts");
        assert_eq!(f.data_type(), &DataType::List(ArrowField::new("element".to_string(),
                                                                  DataType::Struct(vec![
                                                                      ArrowField::new("address".to_string(), DataType::Utf8, true),
                                                                      ArrowField::new("phone".to_string(), DataType::Utf8, true),
                                                                      ArrowField::new("email".to_string(), DataType::Utf8, true), ].into()
                                                                  ), false).into()));

        let f = arrow_schema.field(4);
        assert_eq!(f.name(), "details");
        assert_eq!(f.data_type(), &DataType::Struct(vec![
            ArrowField::new("physical".to_string(), DataType::Struct(vec![
                ArrowField::new("type".to_string(), DataType::Utf8, true),
                ArrowField::new("age".to_string(), DataType::UInt32, true),
                ArrowField::new("created_date".to_string(), DataType::Timestamp(TimeUnit::Millisecond, None), true),
                ArrowField::new("created_by".to_string(), DataType::Utf8, true),
            ].into()), true),
            ArrowField::new("financial".to_string(), DataType::Struct(vec![
                ArrowField::new("type".to_string(), DataType::Utf8, true),
                ArrowField::new("salary".to_string(), DataType::UInt64, true),
                ArrowField::new("created_date".to_string(), DataType::Timestamp(TimeUnit::Millisecond, None), true),
                ArrowField::new("created_by".to_string(), DataType::Utf8, true),
            ].into()), true),
        ].into()));
    }

    #[test]
    fn nested_polymorphic_schema_message_to_json() {
        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person".to_string(), nested_polymorphic_schema().as_slice());
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");

        let expected_json = json!({
                    "id": 1,
                    "name": "John",
                    "status": "ACTIVE",
                    "contacts": [
                        {
                            "address": "123 Main St",
                            "phone": "555-555-5555",
                            "email": "test@test.com"
                            }
                    ],
                    "details": {
                        "physical": {
                            "type": "PHYSICAL",
                            "age": 30,
                            "created_date": JsonValue::Number((1715276726099 as i64).into()),
                            "created_by": "123e4567-e89b-12d3-a456-426614174000"
                        }
                    }
                });

        // Construct physical message value
        let msg_physical = proto_schema.context.get_message("example.details.Physical").unwrap();
        let TypeInfo::Enum(details_type) = proto_schema.context.get_type("example.details.DetailsType.Enum").unwrap()
            else { panic!("Expected enum DetailsType type info") };
        let TypeInfo::Message(timestamp) = proto_schema.context.get_type("google.protobuf.Timestamp").unwrap()
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
                FieldValue {
                    number: 3,
                    value: Value::Message(Box::new(MessageValue {
                        msg_ref: timestamp.self_ref.clone(),
                        garbage: None,
                        fields: vec![
                            FieldValue {
                                number: 1,
                                value: Value::Int64(1715276726),
                            },
                            FieldValue {
                                number: 2,
                                value: Value::Int32(99_000_000), // 99 milliseconds
                            },
                        ]
                    })),
                },
                FieldValue {
                    number: 4,
                    value: Value::String("123e4567-e89b-12d3-a456-426614174000".to_string()),
                }
            ]
        };

        // Construct person message value
        let msg = proto_schema.context.get_message("example.Person").unwrap();
        let msg_detail = proto_schema.context.get_message("example.details.Details").unwrap();
        let TypeInfo::Enum(status) = proto_schema.context.get_type("example.Status.Enum").unwrap() else { panic!("Expected enum Status type info") };

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
                        msg_ref: proto_schema.context.get_message("example.Contact").unwrap().self_ref.clone(),
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

        let proto_value = proto_value.encode(&proto_schema.context());
        let json = proto_schema.decode_to_json(proto_value.as_ref()).unwrap();
        assert_eq!(json, expected_json);
    }
}