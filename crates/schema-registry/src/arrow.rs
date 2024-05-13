use deltalake::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use protofish::context::{Context, MessageField, MessageInfo, Multiplicity, ValueType};

use crate::SchemaRegistryError;

/// Converts a protobuf compiled schema to arrow schema.
/// This function uses the protofish library compiled [`Context`] and top level message [`MessageInfo`].

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

#[cfg(test)]
mod tests {
    use deltalake::arrow::datatypes::{DataType, Field as ArrowField, TimeUnit};

    use crate::proto_schema::tests::{complex_schema, nested_polymorphic_schema, simple_schema_sample};
    use crate::ProtoSchema;

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
}