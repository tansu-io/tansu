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

use std::cmp::Ordering::Equal;
use std::cmp::Ordering::Greater;
use std::cmp::Ordering::Less;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use serde::Deserialize;
use serde::Serialize;

use crate::Result;

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    left: Option<u64>,
    right: Option<u64>,
    value: u64,
    deleted: bool,
}

impl Entry {
    fn size_of() -> usize {
        size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u8>()
    }
}

impl From<Entry> for Bytes {
    fn from(entry: Entry) -> Self {
        let mut encoded = BytesMut::new();
        encoded.reserve(Entry::size_of());
        encoded.put_u64_le(entry.left.unwrap_or(0));
        encoded.put_u64_le(entry.right.unwrap_or(0));
        encoded.put_u64_le(entry.value);
        encoded.put_u8(if entry.deleted { 1 } else { 0 });
        Bytes::from(encoded)
    }
}

impl From<Bytes> for Entry {
    fn from(mut encoded: Bytes) -> Self {
        Self {
            left: match encoded.get_u64_le() {
                0 => None,
                value => Some(value),
            },
            right: match encoded.get_u64_le() {
                0 => None,
                value => Some(value),
            },
            value: encoded.get_u64_le(),
            deleted: encoded.get_u8() == 1,
        }
    }
}

#[allow(dead_code)]
struct Tree<T> {
    inner: T,
}

#[allow(dead_code)]
impl<T> Tree<T>
where
    T: Read + Write + Seek + std::fmt::Debug,
{
    fn write(&mut self, position: u64, entry: Entry) -> Result<()> {
        dbg!(position);
        dbg!(&entry);

        _ = self.inner.seek(SeekFrom::Start(position))?;
        self.inner.write_all(&Bytes::from(entry))?;
        self.inner.flush()?;
        Ok(())
    }

    fn read(&mut self, position: u64) -> Result<Entry> {
        _ = self.inner.seek(SeekFrom::Start(position))?;

        dbg!(position);
        dbg!(&self.inner);
        dbg!(Entry::size_of());
        let mut encoded = BytesMut::zeroed(Entry::size_of());
        self.inner.read_exact(&mut encoded)?;
        dbg!(&encoded);
        Ok(Entry::from(Bytes::from(encoded)))
    }

    fn root(&mut self) -> Result<Entry> {
        self.read(0)
    }

    fn new(inner: T, value: u64) -> Result<Self> {
        let mut myself = Self { inner };
        myself.write(
            0,
            Entry {
                left: None,
                right: None,
                value,
                deleted: false,
            },
        )?;
        Ok(myself)
    }

    fn into_inner(self) -> T {
        self.inner
    }

    fn insert(&mut self, mut entry: Entry, value: u64) -> Result<u64> {
        match value.cmp(&entry.value) {
            Less => {
                if let Some(lhs) = entry.left {
                    let left = self.read(lhs)?;
                    self.insert(left, value)
                } else {
                    let update = self.inner.stream_position()? - (Entry::size_of() as u64);
                    let new = self.inner.seek(SeekFrom::End(0))?;
                    entry.left = Some(new);
                    self.write(
                        new,
                        Entry {
                            left: None,
                            right: None,
                            value,
                            deleted: false,
                        },
                    )?;
                    self.write(update, entry)?;
                    Ok(new)
                }
            }
            Equal => todo!(),
            Greater => {
                if let Some(rhs) = entry.right {
                    let right = self.read(rhs)?;
                    self.insert(right, value)
                } else {
                    let update = self.inner.stream_position()? - (Entry::size_of() as u64);
                    dbg!(update);
                    let new = self.inner.seek(SeekFrom::End(0))?;
                    dbg!(new);
                    entry.right = Some(new);
                    self.write(
                        new,
                        Entry {
                            left: None,
                            right: None,
                            value,
                            deleted: false,
                        },
                    )?;
                    self.write(update, entry)?;
                    Ok(new)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn first() -> Result<()> {
        let mut t = Tree::new(Cursor::new(Vec::new()), 6)?;

        let root = t.root()?;
        assert_eq!(None, root.left);
        assert_eq!(None, root.right);
        assert_eq!(6, root.value);
        assert!(!root.deleted);

        Ok(())
    }

    #[test]
    fn insert_eight() -> Result<()> {
        let mut t = Tree::new(Cursor::new(Vec::new()), 6)?;
        let root = t.root()?;
        assert_eq!(None, root.left);
        assert_eq!(None, root.right);
        assert_eq!(6, root.value);
        assert!(!root.deleted);

        let eight_position = t.insert(root, 8)?;

        let root = t.root()?;
        assert_eq!(None, root.left);
        assert_eq!(Some(eight_position), root.right);
        assert_eq!(6, root.value);
        assert!(!root.deleted);

        let eight = t.read(eight_position)?;
        assert_eq!(None, eight.left);
        assert_eq!(None, eight.right);
        assert_eq!(8, eight.value);
        assert!(!eight.deleted);

        Ok(())
    }
}
