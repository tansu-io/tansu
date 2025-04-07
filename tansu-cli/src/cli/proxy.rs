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

use crate::EnvVarExp;

use super::DEFAULT_BROKER;
use clap::Args;
use url::Url;

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    #[arg(long, env = "LISTENER_URL", default_value = "tcp://[::]:9092")]
    pub(super) listener_url: EnvVarExp<Url>,

    #[arg(long, default_value = DEFAULT_BROKER)]
    pub(super) origin_url: EnvVarExp<Url>,
}
