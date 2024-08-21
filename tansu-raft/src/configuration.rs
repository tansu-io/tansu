// Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

use core::fmt::Debug;
use std::time::Duration;

use url::Url;

use crate::Result;

pub trait Configuration: Debug + Send + Sync {
    fn election_timeout(&self) -> Result<Duration>;
    fn listener_url(&self) -> Result<Url>;
}

pub trait ProvideConfiguration: Send {
    fn provide_configuration(&self) -> Result<Box<dyn Configuration>>;
}
