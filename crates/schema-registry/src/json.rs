use protofish::context::{Context, MessageInfo, Multiplicity};
use protofish::decode::{FieldValue, MessageValue, PackedArray, Value};
use serde_json::{json, to_value, Value as JsonValue};
use crate::SchemaRegistryError;

/// Decode a proto message to a json value.
/// This function uses the protofish library compiled [`Context`], top level message [`MessageInfo`] and the [`MessageValue`] data.
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
    use protofish::context::TypeInfo;
    use protofish::decode::{EnumValue, FieldValue, MessageValue, PackedArray, Value};
    use serde_json::{json, Value as JsonValue};
    use crate::proto_schema::tests::{nested_polymorphic_schema, simple_schema_sample};
    use crate::ProtoSchema;
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
                        ],
                    })),
                },
                FieldValue {
                    number: 4,
                    value: Value::String("123e4567-e89b-12d3-a456-426614174000".to_string()),
                },
            ],
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
                            },
                        ],
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
                        ],
                    })),
                },
            ],
        };

        let proto_value = proto_value.encode(&proto_schema.context());
        let json = proto_schema.decode_to_json(proto_value.as_ref()).unwrap();
        assert_eq!(json, expected_json);
    }
}
