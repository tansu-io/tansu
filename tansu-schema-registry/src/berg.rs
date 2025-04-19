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

use std::env::vars;

use iceberg::{
    io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{Literal, PrimitiveLiteral, PrimitiveType, Schema, Struct, Type},
};
use ordered_float::OrderedFloat;

pub fn env_mapping(k: &str) -> &str {
    match k {
        "AWS_ACCESS_KEY_ID" => S3_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY" => S3_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION" => S3_REGION,
        "AWS_ENDPOINT" => S3_ENDPOINT,
        _ => unreachable!("{k}"),
    }
}

pub fn env_s3_props() -> impl Iterator<Item = (String, String)> {
    vars()
        .filter(|(k, _)| {
            k == "AWS_ACCESS_KEY_ID"
                || k == "AWS_SECRET_ACCESS_KEY"
                || k == "AWS_DEFAULT_REGION"
                || k == "AWS_ENDPOINT"
        })
        .map(|(k, v)| (env_mapping(k.as_str()).to_owned(), v))
}

pub fn partition_value(schema: &Schema) -> Struct {
    Struct::from_iter(schema.as_struct().fields().iter().map(|field| {
        match field.as_ref().field_type.as_ref() {
            Type::Primitive(primitive_type) => primitive_type_partition_value(primitive_type),
            Type::Struct(_struct_type) => todo!(),
            Type::List(_list_type) => todo!(),
            Type::Map(_map_type) => todo!(),
        }
    }))
}

fn primitive_type_partition_value(primitive: &PrimitiveType) -> Option<Literal> {
    match primitive {
        PrimitiveType::Boolean => todo!(),
        PrimitiveType::Int => Some(Literal::Primitive(PrimitiveLiteral::Int(0))),
        PrimitiveType::Long => Some(Literal::Primitive(PrimitiveLiteral::Long(0))),
        PrimitiveType::Float => Some(Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(
            0.0,
        )))),
        PrimitiveType::Double => Some(Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(
            0.0f64,
        )))),
        PrimitiveType::Decimal { .. } => todo!(),
        PrimitiveType::Date => todo!(),
        PrimitiveType::Time => todo!(),
        PrimitiveType::Timestamp => todo!(),
        PrimitiveType::Timestamptz => todo!(),
        PrimitiveType::TimestampNs => todo!(),
        PrimitiveType::TimestamptzNs => todo!(),
        PrimitiveType::String => todo!(),
        PrimitiveType::Uuid => todo!(),
        PrimitiveType::Fixed(_) => todo!(),
        PrimitiveType::Binary => todo!(),
    }
}
