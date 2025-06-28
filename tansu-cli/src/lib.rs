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

use std::{collections::HashMap, env::vars, fmt, result, str::FromStr};

mod cli;

pub use cli::Cli;
use regex::{Regex, Replacer};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Box(#[from] Box<dyn std::error::Error + Send + Sync>),
    Cat(Box<tansu_cat::Error>),
    DotEnv(#[from] dotenv::Error),
    Generate(#[from] tansu_generator::Error),
    Regex(#[from] regex::Error),
    Schema(Box<tansu_schema_registry::Error>),
    Server(Box<tansu_server::Error>),
    Topic(#[from] tansu_topic::Error),
    Url(#[from] url::ParseError),
}

impl From<tansu_cat::Error> for Error {
    fn from(value: tansu_cat::Error) -> Self {
        Self::Cat(Box::new(value))
    }
}

impl From<tansu_schema_registry::Error> for Error {
    fn from(value: tansu_schema_registry::Error) -> Self {
        Self::Schema(Box::new(value))
    }
}

impl From<tansu_server::Error> for Error {
    fn from(value: tansu_server::Error) -> Self {
        Self::Server(Box::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct VarRep(HashMap<String, String>);

impl From<HashMap<String, String>> for VarRep {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

impl VarRep {
    fn replace(&self, haystack: &str) -> Result<String> {
        Regex::new(r"\$\{(?<var>[^\}]+)\}")
            .map(|re| re.replace(haystack, self).into_owned())
            .map_err(Into::into)
    }
}

impl Replacer for &VarRep {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        if let Some(variable) = caps.name("var") {
            if let Some(value) = self.0.get(variable.as_str()) {
                dst.push_str(value);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnvVarExp<T>(T);

impl<T> EnvVarExp<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromStr for EnvVarExp<T>
where
    T: FromStr,
    Error: From<<T as FromStr>::Err>,
{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        VarRep::from(vars().collect::<HashMap<_, _>>())
            .replace(s)
            .and_then(|s| T::from_str(&s).map_err(Into::into))
            .map(|t| Self(t))
    }
}
