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

use crate::Result;
use arrow::datatypes::DataType;
use datafusion::sql::sqlparser::{
    ast::{DataType as SqlDataType, Expr},
    dialect::GenericDialect,
    parser::Parser,
};
use tracing::debug;

pub(crate) fn typeof_sql_expr(expr: &str) -> Result<DataType> {
    let dialect = GenericDialect {};
    match Parser::new(&dialect)
        .try_with_sql(expr)?
        .parse_expr()
        .inspect(|ast| debug!(?ast))?
    {
        Expr::Cast { data_type, .. } => delta_sql_type(data_type),

        otherwise => todo!("{otherwise:?}"),
    }
}

fn delta_sql_type(data_type: SqlDataType) -> Result<DataType> {
    match data_type {
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int32),

        otherwise => todo!("{otherwise:?}"),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

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

    #[test]
    fn simple_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            typeof_sql_expr("cast(meta.timestamp as date)")?,
            DataType::Date32
        );

        Ok(())
    }
}
