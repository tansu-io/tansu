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

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::Term;

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Outcome {
    pub from: Url,
    pub term: Term,
    pub result: bool,
}

impl Outcome {
    pub fn from(&self) -> Url {
        self.from.clone()
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn result(&self) -> bool {
        self.result
    }

    pub fn builder() -> OutcomeBuilder {
        OutcomeBuilder::default()
    }
}

#[derive(
    Copy, Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct OutcomeBuilder<F = PhantomData<Url>, T = PhantomData<Term>, R = PhantomData<bool>> {
    from: F,
    term: T,
    result: R,
}

impl OutcomeBuilder<Url, Term, bool> {
    pub fn build(self) -> Outcome {
        Outcome {
            from: self.from,
            term: self.term,
            result: self.result,
        }
    }
}

impl<F, T, R> OutcomeBuilder<F, T, R> {
    pub fn from(self, from: Url) -> OutcomeBuilder<Url, T, R> {
        OutcomeBuilder {
            from,
            term: self.term,
            result: self.result,
        }
    }
}

impl<F, T, R> OutcomeBuilder<F, T, R> {
    pub fn term(self, term: Term) -> OutcomeBuilder<F, Term, R> {
        OutcomeBuilder {
            from: self.from,
            term,
            result: self.result,
        }
    }
}

impl<F, T, R> OutcomeBuilder<F, T, R> {
    pub fn result(self, result: bool) -> OutcomeBuilder<F, T, bool> {
        OutcomeBuilder {
            from: self.from,
            term: self.term,
            result,
        }
    }
}
