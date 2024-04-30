use protofish::context::{Context, MessageField, MessageInfo, Multiplicity, ValueType};
use deltalake::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
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

    pub fn try_compile_with_full_name(full_name: String, raw_schemas: &[String]) -> Result<Self, SchemaRegistryError> {
        let context = Context::parse(raw_schemas)?;
        Ok(Self {
            context,
            full_name,
        })
    }

    pub fn to_arrow_schema(&self) -> Result<ArrowSchema, SchemaRegistryError> {
        let info = self.context.get_message(&self.full_name)
            .ok_or(SchemaRegistryError::ArrowSchemaGenerationError(format!("Message not found {:?}", self.full_name)))?;
        let schema = to_arrow_schema(&self.context, info)?;
        Ok(schema)
    }
}

pub fn to_arrow_schema(ctx: &Context, info: &MessageInfo) -> Result<ArrowSchema, SchemaRegistryError> {
    let mut fields = vec![];
    for field in info.iter_fields() {
        let field = message_field_to_arrow(ctx, field)?;
        fields.push(field);
    }
    Ok(ArrowSchema::new(fields))
}

fn message_field_to_arrow(ctx: &Context, info: &MessageField) -> Result<ArrowField, SchemaRegistryError> {
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

            //TODO handle google well known types

            let info = ctx.resolve_message(info);
            let mut fields = vec![];
            for f in info.iter_fields() {
                let field = message_field_to_arrow(ctx, f)?;
                fields.push(field);
            }
            DataType::Struct(fields.into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_proto_and_generate_arrow_schema() {
        let raw_schemas = vec![
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
        ];

        let proto_schema = ProtoSchema::try_compile_with_full_name("example.Person".to_string(), &raw_schemas);
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
                ArrowField::new("email".to_string(), DataType::Utf8, true),].into()
            ), false).into()));

        let f = arrow_schema.field(6);
        assert_eq!(f.name(), "wrapped_statuses");
        assert_eq!(f.data_type(), &DataType::List(ArrowField::new("element".to_string(), DataType::Utf8, false).into()));

        let f = arrow_schema.field(7);
        assert_eq!(f.name(), "ids");
        assert_eq!(f.data_type(), &DataType::List(ArrowField::new("element".to_string(), DataType::Int32, false).into()));
    }
}