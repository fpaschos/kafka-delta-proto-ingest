use protofish::context::{Context, MessageField, MessageInfo, ValueType};
use deltalake::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaBuilder as ArrowSchemaBuilder};
use crate::registry::SchemaRegistryError;

#[derive(Debug)]
pub struct ProtoSchema {
    pub(crate) context: Context,
    pub(crate) full_name: String,

}

impl ProtoSchema {
    pub fn try_compile(raw_schemas: &[String]) -> Result<Self, SchemaRegistryError> {
        // TODO find top level full name from last schema
       Self::try_compile_with_full_name("".to_string(), raw_schemas)
    }

    pub fn try_compile_with_full_name(full_name: String, raw_schemas: &[String] ) -> Result<Self, SchemaRegistryError> {
        let context = Context::parse(raw_schemas)?;
        Ok(Self {
            context,
            full_name,
        })
    }

    pub fn to_arrow_schema(&self) -> Result<ArrowSchema, SchemaRegistryError> {
        let builder = ArrowSchemaBuilder::new();
        // builder

        // TODO


        let schema = builder.finish();
        Ok(schema)
    }
}

pub fn to_arrow_schema(ctx: &Context, info: &MessageInfo) -> Result<ArrowSchema, SchemaRegistryError> {

    let mut  fields = vec![];

    for field in info.iter_fields() {
        let field = message_field_to_arrow(ctx, field)?;
        fields.push(field);
    }
    // let fields: Vec<_> = info.iter_fields().map(|field| {
    //     // let field = field.unwrap();
    //     // let field = ArrowField::new(field.name().to_string(), field.data_type().into(), field.is_nullable());
    //     // builder = builder.field(field);
    //
    //     message_to_field(ctx, field)
    // }).collect();
    Ok(ArrowSchema::new(fields))
}

fn message_field_to_arrow(ctx: &Context, info: &MessageField) -> Result<ArrowField, SchemaRegistryError> {
    let field_type: DataType =   match info.field_type {
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
        _ => return Err(SchemaRegistryError::ArrowSchemaError(format!("Unsupported field type {:?}", info.field_type))),
        // ValueType::Message(_) => {}
        // ValueType::Enum(_) => {}
    };

    let field = ArrowField::new(info.name.to_owned(), field_type, true);

    Ok(field)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_schema_test() {
        let raw_schemas = vec![
            r#"
            syntax = "proto3";
            package example;
            message Person {
                int32 id = 1;
                string name = 2;
                string email = 3;
            }
            "#.to_string(),
        ];

        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person".to_string(),&raw_schemas);
        let proto_schema = proto_schema.expect("A valid proto3 raw schema");
        let arrow_schema = proto_schema.to_arrow_schema();
        assert!(arrow_schema.is_ok());
    }

}