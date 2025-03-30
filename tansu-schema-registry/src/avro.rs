// Copyright ⓒ 2025 Peter Morgan <peter.james.morgan@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{collections::HashMap, io::Cursor, iter::zip};

use apache_avro::{
    Reader,
    schema::{ArraySchema, MapSchema, RecordSchema},
    types::Value,
};
use bytes::Bytes;
use datafusion::arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Decimal256Builder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListBuilder,
        MapBuilder, NullBuilder, StringBuilder, StructBuilder, Time32MillisecondBuilder,
        Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, UInt32Builder,
    },
    datatypes::{DataType, Field, FieldRef, Fields, TimeUnit, UnionFields, UnionMode},
    record_batch::RecordBatch,
};
use num_bigint::BigInt;
use tansu_kafka_sans_io::{ErrorCode, record::inflated::Batch};
use tracing::{debug, error};

use crate::{AsArrow, Error, Result, Validator, arrow::RecordBuilder};

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Schema {
    key: Option<apache_avro::Schema>,
    value: Option<apache_avro::Schema>,
}

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

fn schema_data_type(schema: &apache_avro::Schema) -> Result<DataType> {
    debug!(?schema);

    match schema {
        apache_avro::Schema::Null => Ok(DataType::Null),
        apache_avro::Schema::Boolean => Ok(DataType::Boolean),
        apache_avro::Schema::Int => Ok(DataType::Int32),
        apache_avro::Schema::Long => Ok(DataType::Int64),
        apache_avro::Schema::Float => Ok(DataType::Float32),
        apache_avro::Schema::Double => Ok(DataType::Float64),
        apache_avro::Schema::Bytes => Ok(DataType::Binary),
        apache_avro::Schema::String => Ok(DataType::Utf8),

        apache_avro::Schema::Array(schema) => schema_data_type(&schema.items)
            .inspect(|item| debug!(?schema, ?item))
            .map(|item| DataType::new_list(item, NULLABLE)),

        apache_avro::Schema::Map(schema) => schema_data_type(&schema.types)
            .inspect(|value| debug!(?schema, ?value))
            .map(|value| {
                DataType::Map(
                    FieldRef::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from_iter([
                            Field::new("keys", DataType::Utf8, !NULLABLE),
                            Field::new("values", value, NULLABLE),
                        ])),
                        !NULLABLE,
                    )),
                    SORTED_MAP_KEYS,
                )
            }),

        apache_avro::Schema::Union(schema) => {
            debug!(?schema);
            schema
                .variants()
                .iter()
                .enumerate()
                .map(|(index, variant)| {
                    schema_data_type(variant)
                        .map(|data_type| {
                            Field::new(format!("field{}", index + 1), data_type, NULLABLE)
                        })
                        .inspect(|field| debug!(?field))
                })
                .collect::<Result<Vec<_>>>()
                .inspect(|fields| debug!(?fields))
                .and_then(|fields| {
                    i8::try_from(schema.variants().len())
                        .map(|type_ids| {
                            UnionFields::new((1..=type_ids).collect::<Vec<_>>(), fields)
                        })
                        .map_err(Into::into)
                })
                .inspect(|union_fields| debug!(?union_fields))
                .map(|fields| DataType::Union(fields, UnionMode::Dense))
        }

        apache_avro::Schema::Record(schema) => {
            debug!(?schema);
            schema
                .fields
                .iter()
                .map(|field| {
                    schema_data_type(&field.schema)
                        .map(|data_type| Field::new(field.name.clone(), data_type, NULLABLE))
                })
                .collect::<Result<Vec<_>>>()
                .map(Fields::from)
                .map(DataType::Struct)
        }

        apache_avro::Schema::Enum(schema) => {
            debug!(?schema);

            Ok(DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::UInt32),
            ))
        }

        apache_avro::Schema::Fixed(schema) => i32::try_from(schema.size)
            .map(DataType::FixedSizeBinary)
            .map_err(Into::into),

        apache_avro::Schema::Decimal(schema) => u8::try_from(schema.precision)
            .and_then(|precision| {
                i8::try_from(schema.scale).map(|scale| {
                    if precision <= 16 {
                        DataType::Decimal128(precision, scale)
                    } else {
                        DataType::Decimal256(precision, scale)
                    }
                })
            })
            .map_err(Into::into),

        apache_avro::Schema::BigDecimal => todo!(),
        apache_avro::Schema::Uuid => Ok(DataType::Utf8),
        apache_avro::Schema::Date => Ok(DataType::Date32),

        apache_avro::Schema::TimeMillis => Ok(DataType::Time32(TimeUnit::Millisecond)),

        apache_avro::Schema::TimeMicros => Ok(DataType::Time64(TimeUnit::Microsecond)),

        apache_avro::Schema::TimestampMillis => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        }

        apache_avro::Schema::TimestampMicros => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }

        apache_avro::Schema::TimestampNanos => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),

        apache_avro::Schema::LocalTimestampMillis => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        }

        apache_avro::Schema::LocalTimestampMicros => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }

        apache_avro::Schema::LocalTimestampNanos => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }

        apache_avro::Schema::Duration => Ok(DataType::Struct(Fields::from_iter([
            Field::new("month", DataType::UInt32, NULLABLE),
            Field::new("days", DataType::UInt32, NULLABLE),
            Field::new("milliseconds", DataType::UInt32, NULLABLE),
        ]))),

        apache_avro::Schema::Ref { name } => {
            let _ = name;
            todo!();
        }
    }
}

fn schema_array_builder(schema: &apache_avro::Schema) -> Result<Box<dyn ArrayBuilder>> {
    match schema {
        apache_avro::Schema::Null => Ok(Box::new(NullBuilder::new())),
        apache_avro::Schema::Boolean => Ok(Box::new(BooleanBuilder::new())),
        apache_avro::Schema::Int => Ok(Box::new(Int32Builder::new())),
        apache_avro::Schema::Long => Ok(Box::new(Int64Builder::new())),
        apache_avro::Schema::Float => Ok(Box::new(Float32Builder::new())),
        apache_avro::Schema::Double => Ok(Box::new(Float64Builder::new())),
        apache_avro::Schema::Bytes => Ok(Box::new(BinaryBuilder::new())),
        apache_avro::Schema::String => Ok(Box::new(StringBuilder::new())),

        apache_avro::Schema::Array(schema) => schema_array_builder(&schema.items)
            .map(ListBuilder::new)
            .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

        apache_avro::Schema::Map(schema) => schema_array_builder(&schema.types)
            .map(|builder| {
                MapBuilder::new(
                    None,
                    Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                    builder,
                )
            })
            .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

        apache_avro::Schema::Union(schema) => {
            let _ = schema;
            todo!();
        }

        apache_avro::Schema::Record(schema) => schema
            .fields
            .iter()
            .try_fold((vec![], vec![]), |(mut fields, mut builders), field| {
                schema_data_type(&field.schema)
                    .map(|data_type| {
                        fields.push(Field::new(field.name.clone(), data_type, NULLABLE))
                    })
                    .and(schema_array_builder(&field.schema).map(|builder| builders.push(builder)))
                    .map(|()| (fields, builders))
            })
            .map(|(fields, builders)| StructBuilder::new(fields, builders))
            .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

        apache_avro::Schema::Enum(schema) => {
            let _ = schema;
            todo!();
        }

        apache_avro::Schema::Fixed(_schema) => Ok(Box::new(BinaryBuilder::new())),

        apache_avro::Schema::Decimal(schema) => u8::try_from(schema.precision)
            .map(|precision| {
                if precision <= 16 {
                    Box::new(Decimal128Builder::new()) as Box<dyn ArrayBuilder>
                } else {
                    Box::new(Decimal256Builder::new()) as Box<dyn ArrayBuilder>
                }
            })
            .map_err(Into::into),

        apache_avro::Schema::BigDecimal => todo!(),
        apache_avro::Schema::Uuid => Ok(Box::new(StringBuilder::new())),
        apache_avro::Schema::Date => Ok(Box::new(Date32Builder::new())),
        apache_avro::Schema::TimeMillis => Ok(Box::new(Time32MillisecondBuilder::new())),
        apache_avro::Schema::TimeMicros => Ok(Box::new(Time64MicrosecondBuilder::new())),
        apache_avro::Schema::TimestampMillis => Ok(Box::new(TimestampMillisecondBuilder::new())),
        apache_avro::Schema::TimestampMicros => Ok(Box::new(TimestampMicrosecondBuilder::new())),
        apache_avro::Schema::TimestampNanos => Ok(Box::new(TimestampNanosecondBuilder::new())),
        apache_avro::Schema::LocalTimestampMillis => Ok(Box::new(Time32MillisecondBuilder::new())),
        apache_avro::Schema::LocalTimestampMicros => Ok(Box::new(Time64MicrosecondBuilder::new())),
        apache_avro::Schema::LocalTimestampNanos => Ok(Box::new(Time64NanosecondBuilder::new())),

        apache_avro::Schema::Duration => Ok(Box::new(StructBuilder::new(
            vec![
                Field::new("month", DataType::UInt32, NULLABLE),
                Field::new("days", DataType::UInt32, NULLABLE),
                Field::new("milliseconds", DataType::UInt32, NULLABLE),
            ],
            vec![
                Box::new(UInt32Builder::new()),
                Box::new(UInt32Builder::new()),
                Box::new(UInt32Builder::new()),
            ],
        ))),

        apache_avro::Schema::Ref { name } => {
            let _ = name;
            todo!();
        }
    }
}

impl TryFrom<&Schema> for RecordBuilder {
    type Error = Error;

    fn try_from(value: &Schema) -> std::result::Result<Self, Self::Error> {
        let mut keys = vec![];

        if let Some(ref schema) = value.key {
            keys.push(schema_array_builder(schema)?);
        }

        let mut values = vec![];

        if let Some(ref schema) = value.value {
            values.push(schema_array_builder(schema)?)
        }

        Ok(Self { keys, values })
    }
}

macro_rules! try_as {
    ($name:ident, $pattern:path, $type:ty) => {
        fn $name(value: Value) -> Result<$type> {
            if let $pattern(value) = value {
                Ok(value)
            } else {
                Err(Error::InvalidValue(value))
            }
        }
    };
}

try_as!(try_as_i32, Value::Int, i32);
try_as!(try_as_bool, Value::Boolean, bool);
try_as!(try_as_i64, Value::Long, i64);
try_as!(try_as_f32, Value::Float, f32);
try_as!(try_as_f64, Value::Double, f64);
try_as!(try_as_bytes, Value::Bytes, Vec<u8>);
try_as!(try_as_string, Value::String, String);
// try_as!(try_as_map, Value::Map, HashMap<String, Value>);
try_as!(try_as_record, Value::Record, Vec<(String, Value)>);

fn append_list_builder(
    schema: &ArraySchema,
    values: Vec<Value>,
    builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    match schema.items.as_ref() {
        apache_avro::Schema::Null => builder
            .values()
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bool)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_nulls(values.len()))
            })?,

        apache_avro::Schema::Boolean => builder
            .values()
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bool)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        apache_avro::Schema::Int => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        apache_avro::Schema::Long => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        apache_avro::Schema::Float => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_f32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        apache_avro::Schema::Double => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_f64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        apache_avro::Schema::Bytes => builder
            .values()
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bytes)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        apache_avro::Schema::String | apache_avro::Schema::Uuid => builder
            .values()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_string)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        apache_avro::Schema::Array(_schema) => todo!(),
        apache_avro::Schema::Map(_schema) => todo!(),
        apache_avro::Schema::Union(_schema) => todo!(),

        apache_avro::Schema::Record(schema) => builder
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_record)
                    .collect::<Result<Vec<_>>>()
                    .and_then(|values| {
                        values
                            .into_iter()
                            .map(|items| append_struct_builder(schema, items, builder))
                            .collect::<Result<Vec<_>>>()
                    })
            })
            .map(|_| ())?,

        apache_avro::Schema::Enum(_schema) => todo!(),
        apache_avro::Schema::Fixed(_schema) => todo!(),
        apache_avro::Schema::Decimal(_schema) => todo!(),
        apache_avro::Schema::BigDecimal => todo!(),

        apache_avro::Schema::Date => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        apache_avro::Schema::TimeMillis => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        apache_avro::Schema::TimeMicros => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        apache_avro::Schema::TimestampMillis => todo!(),
        apache_avro::Schema::TimestampMicros => todo!(),
        apache_avro::Schema::TimestampNanos => todo!(),
        apache_avro::Schema::LocalTimestampMillis => todo!(),
        apache_avro::Schema::LocalTimestampMicros => todo!(),
        apache_avro::Schema::LocalTimestampNanos => todo!(),
        apache_avro::Schema::Duration => todo!(),
        apache_avro::Schema::Ref { name } => {
            let _ = name;
            todo!()
        }
    }

    builder.append(true);

    Ok(())
}

fn append_map_builder(
    schema: &MapSchema,
    values: HashMap<String, Value>,
    builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    debug!(?schema, ?values);

    for (key, value) in values {
        append_value(None, Value::String(key), builder.keys())?;
        append_value(None, value, builder.values())?;
    }

    builder.append(true).map_err(Into::into)
}

fn append_struct_builder(
    schema: &RecordSchema,
    items: Vec<(String, Value)>,
    builder: &mut StructBuilder,
) -> Result<()> {
    for (index, (field, (name, value))) in zip(schema.fields.as_slice(), items).enumerate() {
        debug!(?index, ?field, ?name, ?value);

        match (&field.schema, value) {
            (apache_avro::Schema::Null, Value::Null) => builder
                .field_builder::<NullBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_null())?,

            (apache_avro::Schema::Boolean, Value::Boolean(value)) => builder
                .field_builder::<BooleanBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::Int, Value::Int(value)) => builder
                .field_builder::<Int32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::Long, Value::Long(value)) => builder
                .field_builder::<Int64Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::Float, Value::Float(value)) => builder
                .field_builder::<Float32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::Double, Value::Double(value)) => builder
                .field_builder::<Float64Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::Bytes, Value::Bytes(value)) => builder
                .field_builder::<BinaryBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::String, Value::String(value)) => builder
                .field_builder::<StringBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::Array(schema), Value::Array(values)) => builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?schema, ?values))
                .and_then(|builder| append_list_builder(schema, values, builder))?,

            (apache_avro::Schema::Map(schema), Value::Map(values)) => builder
                .field_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(index)
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?schema, ?values))
                .and_then(|builder| append_map_builder(schema, values, builder))?,

            (apache_avro::Schema::Union(_union_schema), _) => todo!(),

            (apache_avro::Schema::Record(schema), Value::Record(items)) => builder
                .field_builder::<StructBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .and_then(|builder| append_struct_builder(schema, items, builder))?,

            (apache_avro::Schema::Enum(_enum_schema), _) => todo!(),
            (apache_avro::Schema::Fixed(_fixed_schema), _) => todo!(),
            (apache_avro::Schema::Decimal(_decimal_schema), _) => todo!(),
            (apache_avro::Schema::BigDecimal, _) => todo!(),

            (apache_avro::Schema::Uuid, Value::Uuid(value)) => builder
                .field_builder::<StringBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value.to_string()))?,

            (apache_avro::Schema::Date, Value::Date(value)) => builder
                .field_builder::<Date32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::TimeMillis, Value::TimeMillis(value)) => builder
                .field_builder::<Time32MillisecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::TimeMicros, Value::TimeMicros(value)) => builder
                .field_builder::<Time64MicrosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::TimestampMillis, Value::TimestampMillis(value)) => builder
                .field_builder::<TimestampMillisecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::TimestampMicros, Value::TimeMicros(value)) => builder
                .field_builder::<TimestampMicrosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::TimestampNanos, Value::TimestampNanos(value)) => builder
                .field_builder::<TimestampNanosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (apache_avro::Schema::LocalTimestampMillis, _) => todo!(),
            (apache_avro::Schema::LocalTimestampMicros, _) => todo!(),
            (apache_avro::Schema::LocalTimestampNanos, _) => todo!(),
            (apache_avro::Schema::Duration, _) => todo!(),
            (apache_avro::Schema::Ref { name }, _) => {
                let _ = name;
                todo!();
            }
            (schema, value) => unimplemented!("schema: {schema:?}, value: {value:?}"),
        }
    }

    builder.append(true);
    Ok(())
}

fn append_value(
    schema: Option<&apache_avro::Schema>,
    value: Value,
    column: &mut Box<dyn ArrayBuilder>,
) -> Result<()> {
    debug!(?value);

    match (schema, value) {
        (_, Value::Null) => todo!(),

        (_, Value::Boolean(value)) => column
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Int(value)) => column
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Long(value)) => column
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Float(value)) => column
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Double(value)) => column
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Bytes(value)) => column
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::String(value)) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Fixed(_, value)) => column
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Enum(_, _)) => todo!(),
        (_, Value::Union(_, _value)) => todo!(),

        (Some(apache_avro::Schema::Array(schema)), Value::Array(values)) => column
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| append_list_builder(schema, values, builder)),

        (Some(apache_avro::Schema::Record(schema)), Value::Record(items)) => column
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| append_struct_builder(schema, items, builder)),

        (Some(apache_avro::Schema::Map(schema)), Value::Map(values)) => column
            .as_any_mut()
            .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .inspect(|_| debug!(?schema, ?values))
            .and_then(|builder| append_map_builder(schema, values, builder)),

        (Some(apache_avro::Schema::Date), Value::Date(value)) => column
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (schema, Value::Decimal(value)) => {
            let big_int = BigInt::from(value);
            todo!("schema: {schema:?}, value: {big_int:?}")
        }
        (schema, Value::BigDecimal(value)) => todo!("schema: {schema:?}, value: {value:?}"),

        (_, Value::TimeMillis(value)) => column
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimeMicros(value)) => column
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampMillis(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampMicros(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampNanos(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (schema, Value::LocalTimestampMillis(value)) => {
            todo!("schema: {schema:?}, value: {value:?}")
        }
        (schema, Value::LocalTimestampMicros(value)) => {
            todo!("schema: {schema:?}, value: {value:?}")
        }
        (schema, Value::LocalTimestampNanos(value)) => {
            todo!("schema: {schema:?}, value: {value:?}")
        }
        (schema, Value::Duration(value)) => todo!("schema: {schema:?}, value: {value:?}"),

        (_, Value::Uuid(value)) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value.to_string())),

        (schema, value) => unimplemented!("schema: {schema:?}, value: {value:?}"),
    }
}

fn process(
    schema: Option<&apache_avro::Schema>,
    encoded: Option<Bytes>,
    builders: &mut Vec<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    schema.map_or(Ok(()), |schema| {
        builders
            .iter_mut()
            .next()
            .ok_or(Error::BuilderExhausted)
            .and_then(|builder| {
                encoded
                    .map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
                        Reader::with_schema(schema, &encoded[..])?
                            .next()
                            .transpose()
                            .map_err(Into::into)
                    })
                    .inspect(|value| debug!(?value))
                    .and_then(|value| value.ok_or(Error::Api(ErrorCode::InvalidRecord)))
                    .and_then(|value| append_value(Some(schema), value, builder))
                    .inspect_err(|err| error!(?err, ?schema))
            })
    })
}

impl AsArrow for Schema {
    fn as_arrow(&self, batch: &Batch) -> Result<RecordBatch> {
        debug!(?batch);

        let schema = datafusion::arrow::datatypes::Schema::try_from(self)?;
        debug!(?schema);

        let mut record_builder = RecordBuilder::try_from(self)?;

        debug!(
            keys = record_builder.keys.len(),
            values = record_builder.values.len()
        );

        for record in &batch.records {
            debug!(?record);

            process(
                self.key.as_ref(),
                record.key.clone(),
                &mut record_builder.keys,
            )?;

            process(
                self.value.as_ref(),
                record.value.clone(),
                &mut record_builder.values,
            )?;
        }

        debug!(
            key_rows = ?record_builder.keys.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            value_rows = ?record_builder.values.iter().map(|rows| rows.len()).collect::<Vec<_>>()
        );

        let mut columns = vec![];
        columns.append(&mut record_builder.keys);
        columns.append(&mut record_builder.values);

        debug!(columns = columns.len());

        RecordBatch::try_new(
            schema.into(),
            columns.iter_mut().map(|builder| builder.finish()).collect(),
        )
        .map_err(Into::into)
    }
}

impl TryFrom<&Schema> for Fields {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        let mut fields = vec![];

        if let Some(ref schema) = schema.key {
            schema_data_type(schema)
                .map(|data_type| {
                    Field::new(
                        schema.name().map_or("key", |name| name.name.as_str()),
                        data_type,
                        NULLABLE,
                    )
                })
                .map(|field| fields.push(field))?;
        }

        if let Some(ref schema) = schema.value {
            schema_data_type(schema)
                .map(|data_type| {
                    Field::new(
                        schema.name().map_or("value", |name| name.name.as_str()),
                        data_type,
                        NULLABLE,
                    )
                })
                .map(|field| fields.push(field))?;
        }

        Ok(fields.into())
    }
}

impl TryFrom<&Schema> for datafusion::arrow::datatypes::Schema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Fields::try_from(schema).map(datafusion::arrow::datatypes::Schema::new)
    }
}

fn parse(encoded: Bytes) -> Result<apache_avro::Schema> {
    apache_avro::Schema::parse_reader(&mut Cursor::new(&encoded[..]))
        .map_err(Into::into)
        .inspect(|schema| debug!(?encoded, ?schema))
}

impl Schema {
    pub(crate) fn new(key: Option<Bytes>, value: Option<Bytes>) -> Result<Self> {
        key.map(parse)
            .transpose()
            .and_then(|key| value.map(parse).transpose().map(|value| (key, value)))
            .inspect(|(key, value)| debug!(?key, ?value))
            .map(|(key, value)| Self { key, value })
    }
}

fn validate(validator: Option<&apache_avro::Schema>, encoded: Option<Bytes>) -> Result<()> {
    debug!(?validator, ?encoded);
    validator.map_or(Ok(()), |schema| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            apache_avro::Reader::with_schema(schema, &encoded[..])
                .and_then(|reader| reader.into_iter().next().transpose())
                .inspect(|value| debug!(?value))
                .inspect_err(|err| debug!(?err))
                .map_err(|_| Error::Api(ErrorCode::InvalidRecord))
                .and_then(|value| {
                    value
                        .ok_or(Error::Api(ErrorCode::InvalidRecord))
                        .map(|_value| ())
                })
        })
    })
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))?
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use crate::Registry;

    use super::*;
    use apache_avro::{Decimal, types::Value};
    use datafusion::{arrow::util::pretty::pretty_format_batches, prelude::*};
    use num_bigint::BigInt;
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use serde_json::json;
    use tansu_kafka_sans_io::record::Record;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;
    use uuid::Uuid;

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn key_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}/key.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&json!({
                        "type": "int"
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
                )
                .await?;
        }

        {
            let location = Path::from(format!("{topic}/value.avsc"));
            _ = object_store.put(&location, serde_json::to_vec(&json!({
                "type": "record",
                "name": "Message",
                "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
            }))
            .map(Bytes::from)
            .map(PutPayload::from)?).await?;
        }

        let registry = Registry::new(object_store);

        let key = apache_avro::Schema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn key_and_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}/key.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&json!({
                        "type": "int"
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
                )
                .await?;
        }

        {
            let location = Path::from(format!("{topic}/value.avsc"));
            _ = object_store.put(&location, serde_json::to_vec(&json!({
                        "type": "record",
                        "name": "Message",
                        "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?).await?;
        }

        let registry = Registry::new(object_store);

        let key = apache_avro::Schema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let value = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            let mut record = apache_avro::types::Record::new(&schema).unwrap();
            record.put("name", "alice");
            record.put("email", "alice@example.com");

            writer
                .append(record)
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.into()).value(value.into()))
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn value_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}/key.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&json!({
                        "type": "int"
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
                )
                .await?;
        }

        {
            let location = Path::from(format!("{topic}/value.avsc"));
            _ = object_store.put(&location, serde_json::to_vec(&json!({
                        "type": "record",
                        "name": "Message",
                        "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
                    }))
                    .map(Bytes::from)
                    .map(PutPayload::from)?).await?;
        }

        let registry = Registry::new(object_store);

        let value = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            let mut record = apache_avro::types::Record::new(&schema).unwrap();
            record.put("name", "alice");
            record.put("email", "alice@example.com");

            writer
                .append(record)
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn no_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let registry = Registry::new(InMemory::new());

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[test]
    fn key() -> Result<()> {
        let _guard = init_tracing()?;

        let input = apache_avro::Schema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(input.clone().into()))
            .build()?;

        let s = Schema::new(
            serde_json::to_vec(&json!({
                "type": "int"
            }))
            .map(Bytes::from)
            .map(Some)?,
            None,
        )?;

        s.validate(&batch)
    }

    #[test]
    fn invalid_key() -> Result<()> {
        let _guard = init_tracing()?;

        let input = apache_avro::Schema::parse(&json!({
            "type": "long"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Long(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(input.clone().into()))
            .build()?;

        let s = Schema::new(
            serde_json::to_vec(&json!({
                "type": "string"
            }))
            .map(Bytes::from)
            .map(Some)?,
            None,
        )?;

        assert!(matches!(
            s.validate(&batch),
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));
        Ok(())
    }

    #[test]
    fn simple_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "title", "type": "string"}, {"name": "message", "type": "string"}]
        });

        let schema = apache_avro::Schema::parse(&schema)?;

        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("title", "Lorem ipsum dolor sit amet");
        record.put("message", "consectetur adipiscing elit");

        let mut writer = apache_avro::Writer::new(&schema, vec![]);
        assert!(writer.append(record)? > 0);

        let input = writer.into_inner()?;
        let reader = apache_avro::Reader::with_schema(&schema, &input[..])?;

        let v = reader.into_iter().next().unwrap()?;

        assert_eq!(
            Value::Record(vec![
                (
                    "title".into(),
                    Value::String("Lorem ipsum dolor sit amet".into()),
                ),
                (
                    "message".into(),
                    Value::String("consectetur adipiscing elit".into()),
                ),
            ]),
            v
        );

        Ok(())
    }

    #[tokio::test]
    async fn record_of_primitive_data_types() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [
                {"name": "a", "type": "null"},
                {"name": "b", "type": "boolean"},
                {"name": "c", "type": "int"},
                {"name": "d", "type": "long"},
                {"name": "e", "type": "float"},
                {"name": "f", "type": "double"},
                {"name": "g", "type": "bytes"},
                {"name": "h", "type": "string"}
            ]
        }))?;

        let values = [r(
            &value_schema,
            [
                ("a", Value::Null),
                ("b", false.into()),
                ("c", i32::MAX.into()),
                ("d", i64::MAX.into()),
                ("e", f32::MAX.into()),
                ("f", f64::MAX.into()),
                ("g", Vec::from(&b"abcdef"[..]).into()),
                ("h", "pqr".into()),
            ],
        )];

        let mut batch = Batch::builder();
        for value in values {
            batch = batch
                .record(Record::builder().value(schema_write(&value_schema, value.into())?.into()))
        }
        let batch = batch.build()?;

        let schema = Schema::new(None, Some(Bytes::from(value_schema.canonical_form())))?;
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-----------------------------------------------------------------------------------------------------------------------------+",
            "| Message                                                                                                                     |",
            "+-----------------------------------------------------------------------------------------------------------------------------+",
            "| {a: , b: false, c: 2147483647, d: 9223372036854775807, e: 3.4028235e38, f: 1.7976931348623157e308, g: 616263646566, h: pqr} |",
            "+-----------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn record_of_with_list_of_primitive_data_types() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [
                {"name": "b", "type": "array", "items": "boolean"},
                {"name": "c", "type": "array", "items": "int"},
                {"name": "d", "type": "array", "items": "long"},
                {"name": "e", "type": "array", "items": "float"},
                {"name": "f", "type": "array", "items": "double"},
                {"name": "g", "type": "array", "items": "bytes"},
                {"name": "h", "type": "array", "items": "string"}
            ]
        }))?;

        let values = [r(
            &value_schema,
            [
                ("b", Value::Array(vec![false.into(), true.into()])),
                (
                    "c",
                    Value::Array(vec![i32::MIN.into(), 0.into(), i32::MAX.into()]),
                ),
                (
                    "d",
                    Value::Array(vec![i64::MIN.into(), 0.into(), i64::MAX.into()]),
                ),
                (
                    "e",
                    Value::Array(vec![f32::MIN.into(), 0.0f32.into(), f32::MAX.into()]),
                ),
                (
                    "f",
                    Value::Array(vec![f64::MIN.into(), 0.0f64.into(), f64::MAX.into()]),
                ),
                ("g", Value::Array(vec![Vec::from(&b"abcdef"[..]).into()])),
                (
                    "h",
                    Value::Array(vec!["abc".into(), "pqr".into(), "xyz".into()]),
                ),
            ],
        )];

        let mut batch = Batch::builder();
        for value in values {
            batch = batch
                .record(Record::builder().value(schema_write(&value_schema, value.into())?.into()))
        }
        let batch = batch.build()?;

        let schema = Schema::new(None, Some(Bytes::from(value_schema.canonical_form())))?;
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| Message                                                                                                                                                                                                                                         |",
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {b: [false, true], c: [-2147483648, 0, 2147483647], d: [-9223372036854775808, 0, 9223372036854775807], e: [-3.4028235e38, 0.0, 3.4028235e38], f: [-1.7976931348623157e308, 0.0, 1.7976931348623157e308], g: [616263646566], h: [abc, pqr, xyz]} |",
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[test]
    fn union_with_ref() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "record",
            "name": "LongList",
            "fields": [{"name": "next", "type": ["null", "LongList"]}]
        });

        let data_type = apache_avro::Schema::parse(&schema)
            .map_err(Into::into)
            .and_then(|schema| schema_data_type(&schema))?;

        assert!(matches!(data_type, DataType::Struct(_)));

        let record = match data_type {
            DataType::Struct(record) => Some(record),
            _ => None,
        }
        .unwrap();

        let next = record[0].clone();
        assert_eq!("next", next.name());
        assert!(matches!(next.data_type(), DataType::Union(_, _)));

        Ok(())
    }

    #[test]
    fn union() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "record",
            "name": "union",
            "fields": [{"name": "next", "type": ["null", "float"]}]
        });

        let data_type = apache_avro::Schema::parse(&schema)
            .map_err(Into::into)
            .and_then(|schema| schema_data_type(&schema))?;

        assert!(matches!(data_type, DataType::Struct(_)));

        let record = match data_type {
            DataType::Struct(record) => Some(record),
            _ => None,
        }
        .unwrap();

        let next = record[0].clone();
        assert_eq!("next", next.name());
        assert!(matches!(
            next.data_type(),
            DataType::Union(_, UnionMode::Dense)
        ));

        let union = match next.data_type() {
            DataType::Union(fields, _) => Some(fields),
            _ => None,
        }
        .unwrap();

        let mut i = union.iter();

        let (index, field) = i.next().unwrap();
        assert_eq!(1, index);
        assert_eq!("field1", field.name());
        assert_eq!(&DataType::Null, field.data_type());

        let (index, field) = i.next().unwrap();
        assert_eq!(2, index);
        assert_eq!("field2", field.name());
        assert_eq!(&DataType::Float32, field.data_type());

        assert!(i.next().is_none());

        Ok(())
    }

    #[test]
    fn enumeration() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "enum",
            "name": "Suit",
            "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        });

        let data_type = apache_avro::Schema::parse(&schema)
            .map_err(Into::into)
            .and_then(|schema| schema_data_type(&schema))?;

        assert!(matches!(data_type, DataType::Dictionary(_, _)));

        let (key_type, value_type) = match data_type {
            DataType::Dictionary(key_type, value_type) => Some((key_type, value_type)),
            _ => None,
        }
        .unwrap();

        assert_eq!(DataType::Utf8, *key_type);
        assert_eq!(DataType::UInt32, *value_type);

        Ok(())
    }

    #[test]
    fn array() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "array",
            "items": "string",
            "default": []
        });

        let data_type = apache_avro::Schema::parse(&schema)
            .map_err(Into::into)
            .and_then(|schema| schema_data_type(&schema))?;

        assert!(matches!(data_type, DataType::List(_)));

        let field = match data_type {
            DataType::List(field) => Some(field),
            _ => None,
        }
        .unwrap();

        assert_eq!("item", field.name());
        assert_eq!(&DataType::Utf8, field.data_type());

        Ok(())
    }

    #[tokio::test]
    async fn map() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = apache_avro::Schema::parse(&json!({
            "type": "map",
            "values": "long",
            "default": {}
        }))?;

        let values = [Value::from(json!({"a": 1, "b": 3, "c": 5}))];

        let mut batch = Batch::builder();
        for value in values {
            batch =
                batch.record(Record::builder().value(schema_write(&value_schema, value)?.into()))
        }
        let batch = batch.build()?;

        let schema = Schema::new(None, Some(Bytes::from(value_schema.canonical_form())))?;
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        fn sort(s: &str) -> String {
            let mut chars = s.chars().collect::<Vec<_>>();
            chars.sort();
            chars.into_iter().collect()
        }

        let expected = vec![
            "+--------------------+",
            "| value              |",
            "+--------------------+",
            "| {c: 5, a: 1, b: 3} |",
            "+--------------------+",
        ]
        .into_iter()
        .map(sort)
        .collect::<Vec<_>>();

        assert_eq!(
            pretty_results.trim().lines().map(sort).collect::<Vec<_>>(),
            expected
        );

        Ok(())
    }

    #[test]
    fn fixed() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "fixed",
            "size": 16,
            "name": "md5"
        });

        let data_type = apache_avro::Schema::parse(&schema)
            .map_err(Into::into)
            .and_then(|schema| schema_data_type(&schema))?;

        assert!(matches!(data_type, DataType::FixedSizeBinary(16)));

        Ok(())
    }

    fn schema_write(schema: &apache_avro::Schema, value: Value) -> Result<Bytes> {
        let mut writer = apache_avro::Writer::new(schema, vec![]);
        writer.append(value)?;
        writer.into_inner().map(Bytes::from).map_err(Into::into)
    }

    #[tokio::test]
    async fn simple_integer_key_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let key_schema = apache_avro::Schema::parse(&json!({
            "type": "int",
        }))?;

        let keys = [32123, 45654, 87678, 12321];

        let batch = {
            let mut batch = Batch::builder();

            for key in keys {
                batch = batch
                    .record(Record::builder().key(schema_write(&key_schema, key.into())?.into()));
            }

            batch.build()
        }?;

        let schema = Schema::new(Some(Bytes::from(key_schema.canonical_form())), None)?;
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-------+",
            "| key   |",
            "+-------+",
            "| 32123 |",
            "| 45654 |",
            "| 87678 |",
            "| 12321 |",
            "+-------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    fn r<'a>(
        schema: &apache_avro::Schema,
        fields: impl IntoIterator<Item = (&'a str, Value)>,
    ) -> apache_avro::types::Record {
        apache_avro::types::Record::new(schema)
            .map(|mut record| {
                for (name, value) in fields {
                    record.put(name, value);
                }
                record
            })
            .unwrap()
    }

    #[tokio::test]
    async fn simple_record_value_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "lucky", "type": "array", "items": "int", "default": []}
            ]
        }))?;

        let values = [
            r(
                &value_schema,
                [
                    ("id", 32123.into()),
                    ("name", "alice".into()),
                    ("lucky", Value::Array([6.into()].into())),
                ],
            ),
            r(
                &value_schema,
                [
                    ("id", 45654.into()),
                    ("name", "bob".into()),
                    ("lucky", Value::Array([5.into(), 9.into()].into())),
                ],
            ),
        ];

        let mut batch = Batch::builder();
        for value in values {
            batch = batch
                .record(Record::builder().value(schema_write(&value_schema, value.into())?.into()))
        }
        let batch = batch.build()?;

        let schema = Schema::new(None, Some(Bytes::from(value_schema.canonical_form())))?;
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------------------------+",
            "| Person                                |",
            "+---------------------------------------+",
            "| {id: 32123, name: alice, lucky: [6]}  |",
            "| {id: 45654, name: bob, lucky: [5, 9]} |",
            "+---------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_bool_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": "boolean",
            "default": []
        });

        let values = [[true, true], [false, true], [true, false], [false, false]]
            .into_iter()
            .map(|l| Value::Array(l.into_iter().map(Value::Boolean).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+----------------+",
            "| value          |",
            "+----------------+",
            "| [true, true]   |",
            "| [false, true]  |",
            "| [true, false]  |",
            "| [false, false] |",
            "+----------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_int_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": "int",
            "default": []
        });

        let values = [vec![32123, 23432, 12321, 56765], vec![i32::MIN, i32::MAX]]
            .into_iter()
            .map(|l| Value::Array(l.into_iter().map(Value::Int).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+------------------------------+",
            "| value                        |",
            "+------------------------------+",
            "| [32123, 23432, 12321, 56765] |",
            "| [-2147483648, 2147483647]    |",
            "+------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_long_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": "long",
            "default": []
        });

        let values = [vec![32123, 23432, 12321, 56765], vec![i64::MIN, i64::MAX]]
            .into_iter()
            .map(|l| Value::Array(l.into_iter().map(Value::Long).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------------------------------+",
            "| value                                       |",
            "+---------------------------------------------+",
            "| [32123, 23432, 12321, 56765]                |",
            "| [-9223372036854775808, 9223372036854775807] |",
            "+---------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_float_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": "float",
            "default": []
        });

        let values = [
            vec![3.2123, 23.432, 123.21, 5676.5],
            vec![f32::MIN, f32::MAX],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::Float).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+----------------------------------+",
            "| value                            |",
            "+----------------------------------+",
            "| [3.2123, 23.432, 123.21, 5676.5] |",
            "| [-3.4028235e38, 3.4028235e38]    |",
            "+----------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_double_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": "double",
            "default": []
        });

        let values = [
            vec![3.2123, 23.432, 123.21, 5676.5],
            vec![f64::MIN, f64::MAX],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::Double).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------------------------------------+",
            "| value                                             |",
            "+---------------------------------------------------+",
            "| [3.2123, 23.432, 123.21, 5676.5]                  |",
            "| [-1.7976931348623157e308, 1.7976931348623157e308] |",
            "+---------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_string_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": "string",
            "default": []
        });

        let values = [
            vec!["abc".to_string(), "def".to_string(), "pqr".to_string()],
            vec!["xyz".to_string()],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::String).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-----------------+",
            "| value           |",
            "+-----------------+",
            "| [abc, def, pqr] |",
            "| [xyz]           |",
            "+-----------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_record_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": {"type": "record",
                      "name": "xyz",
                      "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]},
            "default": []
        });

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            let values = [
                Value::Array(vec![
                    Value::Record(vec![
                        ("id".into(), 32123.into()),
                        ("name".into(), "alice".into()),
                    ]),
                    Value::Record(vec![
                        ("id".into(), 45654.into()),
                        ("name".into(), "bob".into()),
                    ]),
                ]),
                Value::Array(vec![Value::Record(vec![
                    ("id".into(), 54345.into()),
                    ("name".into(), "betty".into()),
                ])]),
            ];

            let mut batch = Batch::builder();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+----------------------------------------------------+",
            "| value                                              |",
            "+----------------------------------------------------+",
            "| [{id: 32123, name: alice}, {id: 45654, name: bob}] |",
            "| [{id: 54345, name: betty}]                         |",
            "+----------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_bytes_value() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "array",
            "items": "bytes",
            "default": []
        });

        let values = [
            vec![b"abc".to_vec(), b"def".to_vec(), b"pqr".to_vec()],
            vec![b"54345".to_vec()],
        ]
        .into_iter()
        .map(|l| Value::Array(l.into_iter().map(Value::Bytes).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------------------+",
            "| value                    |",
            "+--------------------------+",
            "| [616263, 646566, 707172] |",
            "| [3534333435]             |",
            "+--------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn uuid_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "string", "logicalType": "uuid"
        });

        let values = [
            "383BB977-7D38-42B5-8BE7-58A1C606DE7A",
            "2C1FDDC8-4EBE-43FD-8F1C-47E18B7A4E21",
            "F9B45334-9AA2-4978-8735-9800D27A551C",
        ]
        .into_iter()
        .map(|uuid| Uuid::parse_str(uuid).map(Value::Uuid).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;

        let batch = {
            let mut batch = Batch::builder();
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------------------------------+",
            "| value                                |",
            "+--------------------------------------+",
            "| 383bb977-7d38-42b5-8be7-58a1c606de7a |",
            "| 2c1fddc8-4ebe-43fd-8f1c-47e18b7a4e21 |",
            "| f9b45334-9aa2-4978-8735-9800d27a551c |",
            "+--------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn time_millis_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "int", "logicalType": "time-millis"
        });

        let values = [1, 2, 3]
            .into_iter()
            .map(Value::TimeMillis)
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------+",
            "| value        |",
            "+--------------+",
            "| 00:00:00.001 |",
            "| 00:00:00.002 |",
            "| 00:00:00.003 |",
            "+--------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn time_micros_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "long", "logicalType": "time-micros"
        });

        let values = [1, 2, 3]
            .into_iter()
            .map(Value::TimeMicros)
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-----------------+",
            "| value           |",
            "+-----------------+",
            "| 00:00:00.000001 |",
            "| 00:00:00.000002 |",
            "| 00:00:00.000003 |",
            "+-----------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_millis_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "long", "logicalType": "timestamp-millis"
        });

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::TimestampMillis(seconds * 1_000))
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------+",
            "| value               |",
            "+---------------------+",
            "| 1973-10-17T18:36:57 |",
            "| 2001-09-09T01:46:40 |",
            "| 2009-02-13T23:31:30 |",
            "+---------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_micros_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "long", "logicalType": "timestamp-micros"
        });

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::TimestampMicros(seconds * 1_000 * 1_000))
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------+",
            "| value               |",
            "+---------------------+",
            "| 1973-10-17T18:36:57 |",
            "| 2001-09-09T01:46:40 |",
            "| 2009-02-13T23:31:30 |",
            "+---------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn local_timestamp_millis_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "long", "logicalType": "local-timestamp-millis"
        });

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::LocalTimestampMillis(seconds * 1_000))
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------+",
            "| value               |",
            "+---------------------+",
            "| 1973-10-17T18:36:57 |",
            "| 2001-09-09T01:46:40 |",
            "| 2009-02-13T23:31:30 |",
            "+---------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn local_timestamp_micros_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "long", "logicalType": "local-timestamp-micros"
        });

        let values = [119_731_017, 1_000_000_000, 1_234_567_890]
            .into_iter()
            .map(|seconds| Value::LocalTimestampMicros(seconds * 1_000 * 1_000))
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------+",
            "| value               |",
            "+---------------------+",
            "| 1973-10-17T18:36:57 |",
            "| 2001-09-09T01:46:40 |",
            "| 2009-02-13T23:31:30 |",
            "+---------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn date_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "int", "logicalType": "date"
        });

        let values = [
            Value::Int(1),
            Value::Int(1_385),
            Value::Int(11_574),
            Value::Int(14_288),
        ];

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+------------+",
            "| value      |",
            "+------------+",
            "| 1970-01-02 |",
            "| 1973-10-17 |",
            "| 2001-09-09 |",
            "| 2009-02-13 |",
            "+------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn decimal_fixed_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": {"type": "fixed",
                     "size": 8,
                     "name": "decimal"},
            "logicalType": "decimal",
            "precision": 8,
            "scale": 2,
        });

        let values = [32123, 45654, 87678, 12321]
            .into_iter()
            .map(BigInt::from)
            .map(|big_int| big_int.to_signed_bytes_be())
            .map(Decimal::from)
            .map(Value::Decimal)
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+------------+",
            "| value      |",
            "+------------+",
            "| 1970-01-02 |",
            "| 1973-10-17 |",
            "| 2001-09-09 |",
            "| 2009-02-13 |",
            "+------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn decimal_variable_logical_type() -> Result<()> {
        let _guard = init_tracing()?;

        let value_schema = json!({
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 8,
            "scale": 2,
        });

        let values = [32123, 45654, 87678, 12321]
            .into_iter()
            .map(BigInt::from)
            .map(|big_int| big_int.to_signed_bytes_be())
            .map(Decimal::from)
            .map(Value::Decimal)
            .collect::<Vec<_>>();

        let mut batch = Batch::builder();

        let batch = {
            let value_schema =
                apache_avro::Schema::parse(&value_schema).inspect(|schema| debug!(?schema))?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(&value_schema, value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        debug!(?batch);

        let schema = serde_json::to_vec(&value_schema)
            .map_err(Into::into)
            .map(Bytes::from)
            .map(Some)
            .and_then(|value| Schema::new(None, value))?;

        debug!(?schema);

        let record_batch = schema.as_arrow(&batch)?;
        debug!(?record_batch);

        let ctx = SessionContext::new();

        _ = ctx.register_batch("t", record_batch)?;
        let df = ctx.sql("select * from t").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+------------+",
            "| value      |",
            "+------------+",
            "| 1970-01-02 |",
            "| 1973-10-17 |",
            "| 2001-09-09 |",
            "| 2009-02-13 |",
            "+------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn string_key_with_record_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let key_schema = apache_avro::Schema::parse(&json!({
            "type": "string",
        }))?;

        let value_schema = apache_avro::Schema::parse(&json!({
            "type": "record",
            "name": "Grade",
            "fields": [
                {"name": "first", "type": "string"},
                {"name": "last", "type": "string"},
                {"name": "test1", "type": "double"},
                {"name": "test2", "type": "double"},
                {"name": "test3", "type": "double"},
                {"name": "test4", "type": "double"},
                {"name": "final", "type": "double"},
                {"name": "grade", "type": "string"}
            ]
        }))?;

        // https://people.math.sc.edu/Burkardt/datasets/csv/csv.html
        let grades = [
            (
                "Alfalfa",
                "Aloysius",
                "123-45-6789",
                40.0,
                90.0,
                100.0,
                83.0,
                49.0,
                "D-",
            ),
            (
                "Alfred",
                "University",
                "123-12-1234",
                41.0,
                97.0,
                96.0,
                97.0,
                48.0,
                "D+",
            ),
            (
                "Gerty",
                "Gramma",
                "567-89-0123",
                41.0,
                80.0,
                60.0,
                40.0,
                44.0,
                "C",
            ),
            (
                "Android",
                "Electric",
                "087-65-4321",
                42.0,
                23.0,
                36.0,
                45.0,
                47.0,
                "B-",
            ),
            (
                "Bumpkin",
                "Fred",
                "456-78-9012",
                43.0,
                78.0,
                88.0,
                77.0,
                45.0,
                "A-",
            ),
            (
                "Rubble",
                "Betty",
                "234-56-7890",
                44.0,
                90.0,
                80.0,
                90.0,
                46.0,
                "C-",
            ),
            (
                "Noshow",
                "Cecil",
                "345-67-8901",
                45.0,
                11.0,
                -1.0,
                4.0,
                43.0,
                "F",
            ),
            (
                "Buff",
                "Bif",
                "632-79-9939",
                46.0,
                20.0,
                30.0,
                40.0,
                50.0,
                "B+",
            ),
            (
                "Airpump",
                "Andrew",
                "223-45-6789",
                49.0,
                1.0,
                90.0,
                100.0,
                83.0,
                "A",
            ),
            (
                "Backus",
                "Jim",
                "143-12-1234",
                48.0,
                1.0,
                97.0,
                96.0,
                97.0,
                "A+",
            ),
            (
                "Carnivore",
                "Art",
                "565-89-0123",
                44.0,
                1.0,
                80.0,
                60.0,
                40.0,
                "D+",
            ),
            (
                "Dandy",
                "Jim",
                "087-75-4321",
                47.0,
                1.0,
                23.0,
                36.0,
                45.0,
                "C+",
            ),
            (
                "Elephant",
                "Ima",
                "456-71-9012",
                45.0,
                1.0,
                78.0,
                88.0,
                77.0,
                "B-",
            ),
            (
                "Franklin",
                "Benny",
                "234-56-2890",
                50.0,
                1.0,
                90.0,
                80.0,
                90.0,
                "B-",
            ),
            (
                "George",
                "Boy",
                "345-67-3901",
                40.0,
                1.0,
                11.0,
                -1.0,
                4.0,
                "B",
            ),
            (
                "Heffalump",
                "Harvey",
                "632-79-9439",
                30.0,
                1.0,
                20.0,
                30.0,
                40.0,
                "C",
            ),
        ];

        let batch = {
            let mut batch = Batch::builder();

            for grade in grades {
                let mut value = apache_avro::types::Record::new(&value_schema).unwrap();
                value.put("first", grade.0);
                value.put("last", grade.1);
                value.put("test1", grade.3);
                value.put("test2", grade.4);
                value.put("test3", grade.5);
                value.put("test4", grade.6);
                value.put("final", grade.7);
                value.put("grade", grade.8);

                batch = batch.record(
                    Record::builder()
                        .key(schema_write(&key_schema, grade.2.into())?.into())
                        .value(schema_write(&value_schema, value.into())?.into()),
                );
            }

            batch.build()
        }?;

        let schema = Schema::new(
            Some(Bytes::from(key_schema.canonical_form())),
            Some(Bytes::from(value_schema.canonical_form())),
        )?;
        let record_batch = schema.as_arrow(&batch)?;

        let ctx = SessionContext::new();

        _ = ctx.register_batch("search", record_batch)?;
        let df = ctx.sql("select * from search").await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-------------+---------------------------------------------------------------------------------------------------------------+",
            "| key         | Grade                                                                                                         |",
            "+-------------+---------------------------------------------------------------------------------------------------------------+",
            "| 123-45-6789 | {first: Alfalfa, last: Aloysius, test1: 40.0, test2: 90.0, test3: 100.0, test4: 83.0, final: 49.0, grade: D-} |",
            "| 123-12-1234 | {first: Alfred, last: University, test1: 41.0, test2: 97.0, test3: 96.0, test4: 97.0, final: 48.0, grade: D+} |",
            "| 567-89-0123 | {first: Gerty, last: Gramma, test1: 41.0, test2: 80.0, test3: 60.0, test4: 40.0, final: 44.0, grade: C}       |",
            "| 087-65-4321 | {first: Android, last: Electric, test1: 42.0, test2: 23.0, test3: 36.0, test4: 45.0, final: 47.0, grade: B-}  |",
            "| 456-78-9012 | {first: Bumpkin, last: Fred, test1: 43.0, test2: 78.0, test3: 88.0, test4: 77.0, final: 45.0, grade: A-}      |",
            "| 234-56-7890 | {first: Rubble, last: Betty, test1: 44.0, test2: 90.0, test3: 80.0, test4: 90.0, final: 46.0, grade: C-}      |",
            "| 345-67-8901 | {first: Noshow, last: Cecil, test1: 45.0, test2: 11.0, test3: -1.0, test4: 4.0, final: 43.0, grade: F}        |",
            "| 632-79-9939 | {first: Buff, last: Bif, test1: 46.0, test2: 20.0, test3: 30.0, test4: 40.0, final: 50.0, grade: B+}          |",
            "| 223-45-6789 | {first: Airpump, last: Andrew, test1: 49.0, test2: 1.0, test3: 90.0, test4: 100.0, final: 83.0, grade: A}     |",
            "| 143-12-1234 | {first: Backus, last: Jim, test1: 48.0, test2: 1.0, test3: 97.0, test4: 96.0, final: 97.0, grade: A+}         |",
            "| 565-89-0123 | {first: Carnivore, last: Art, test1: 44.0, test2: 1.0, test3: 80.0, test4: 60.0, final: 40.0, grade: D+}      |",
            "| 087-75-4321 | {first: Dandy, last: Jim, test1: 47.0, test2: 1.0, test3: 23.0, test4: 36.0, final: 45.0, grade: C+}          |",
            "| 456-71-9012 | {first: Elephant, last: Ima, test1: 45.0, test2: 1.0, test3: 78.0, test4: 88.0, final: 77.0, grade: B-}       |",
            "| 234-56-2890 | {first: Franklin, last: Benny, test1: 50.0, test2: 1.0, test3: 90.0, test4: 80.0, final: 90.0, grade: B-}     |",
            "| 345-67-3901 | {first: George, last: Boy, test1: 40.0, test2: 1.0, test3: 11.0, test4: -1.0, final: 4.0, grade: B}           |",
            "| 632-79-9439 | {first: Heffalump, last: Harvey, test1: 30.0, test2: 1.0, test3: 20.0, test4: 30.0, final: 40.0, grade: C}    |",
            "+-------------+---------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }
}
