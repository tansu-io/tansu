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

use object_store::{ObjectMeta, PutResult, UpdateVersion};

use crate::Version;

impl From<Version> for UpdateVersion {
    fn from(value: Version) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<ObjectMeta> for Version {
    fn from(value: ObjectMeta) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<UpdateVersion> for Version {
    fn from(value: UpdateVersion) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<PutResult> for Version {
    fn from(value: PutResult) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}
