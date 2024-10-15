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

use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use serde_json::Value;
use std::{
    collections::HashMap,
    env, error,
    fmt::{self, Display},
    fs,
    io::{self, BufRead, BufReader, Cursor, Seek, Write},
    path::Path,
};
use syn::{Expr, Type};
use tansu_kafka_model::{wv::Wv, CommonStruct, Field, Message};

#[derive(Debug)]
#[allow(dead_code)]
enum Error {
    ExpectingArrayExpr(Expr),
    ExpectingPathExpr(Expr),
    ExpectingPathIdentExpr(Expr),
    ExpectingTupleExpr(Expr),
    Glob(glob::GlobError),
    Io(io::Error),
    Json(serde_json::Error),
    KafkaModel(tansu_kafka_model::Error),
    Pattern(glob::PatternError),
    Syn(syn::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<glob::GlobError> for Error {
    fn from(value: glob::GlobError) -> Self {
        Error::Glob(value)
    }
}

impl From<glob::PatternError> for Error {
    fn from(value: glob::PatternError) -> Self {
        Error::Pattern(value)
    }
}

impl From<syn::Error> for Error {
    fn from(value: syn::Error) -> Self {
        Error::Syn(value)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::Json(value)
    }
}

impl From<tansu_kafka_model::Error> for Error {
    fn from(value: tansu_kafka_model::Error) -> Self {
        Error::KafkaModel(value)
    }
}

impl error::Error for Error {}

type Result<T, E = Error> = std::result::Result<T, E>;

fn read_value<P>(filename: P) -> Result<Value>
where
    P: AsRef<Path>,
{
    fs::File::open(filename)
        .map(BufReader::new)
        .and_then(|r| {
            r.lines()
                .try_fold(Cursor::new(Vec::new()), |mut acc, line| {
                    if let Ok(mut line) = line {
                        if let Some(position) = line.find("//") {
                            line.truncate(position);
                        }
                        acc.write_all(line.as_bytes())?;
                    }
                    Ok(acc)
                })
        })
        .and_then(|mut r| r.rewind().map(|()| r))
        .and_then(|r| serde_json::from_reader(r).map_err(Into::into))
        .map_err(Into::into)
}

fn kind(
    parent: Option<&Field>,
    module: &syn::Path,
    f: &Field,
    dependencies: &[Type],
) -> TokenStream {
    #[cfg(feature = "diagnostics")]
    eprintln!(
        "module: {}, field: {}, dependencies: {:?}, primitive: {}, nullable.is_none: {}, \
         versions.is_mandatory: {}",
        module.to_token_stream(),
        f.name(),
        dependencies
            .iter()
            .map(|t| t.to_token_stream().to_string())
            .collect::<Vec<String>>(),
        f.kind().is_primitive(),
        f.nullable().is_none(),
        f.versions()
            .is_mandatory(parent.map(|parent| parent.versions()))
    );

    if f.tag().is_some() {
        let t = f.kind().type_name();

        if f.kind().is_sequence() {
            if f.kind().is_sequence_of_primitive() {
                quote! {
                    Option<Vec<#t>>
                }
            } else {
                quote! {
                    Option<Vec<#module::#t>>
                }
            }
        } else if parent.is_none() && dependencies.contains(&t) {
            quote! {
                Option<#module::#t>
            }
        } else {
            quote! {
                Option<#t>
            }
        }
    } else if f.kind().is_sequence() {
        let t = f.kind().type_name();

        if parent.is_none() && dependencies.contains(&t) {
            quote! {
                Option<Vec<#module::#t>>
            }
        } else {
            quote! {
                Option<Vec<#t>>
            }
        }
    } else if f.kind().is_primitive() {
        let t = f.kind().type_name();

        if f.nullable().is_none() && f.versions().is_mandatory(parent.map(Field::versions)) {
            quote! {
                #t
            }
        } else {
            quote! {
                Option<#t>
            }
        }
    } else {
        let t = f.kind().type_name();
        if f.nullable().is_none() && f.versions().is_mandatory(parent.map(Field::versions)) {
            if parent.is_none() && dependencies.contains(&t) {
                quote! {
                    #module::#t
                }
            } else {
                quote! {
                    #t
                }
            }
        } else if parent.is_none() && dependencies.contains(&t) {
            quote! {
                Option<#module::#t>
            }
        } else {
            quote! {
                Option<#t>
            }
        }
    }
}

fn tag_kind(
    _parent: Option<&Field>,
    module: &syn::Path,
    f: &Field,
    _dependencies: &[Type],
) -> TokenStream {
    #[cfg(feature = "diagnostics")]
    eprintln!(
        "module: {}, field: {}, primitive: {}, nullable.is_none: {}",
        module.to_token_stream(),
        f.name(),
        f.kind().is_primitive(),
        f.nullable().is_none(),
    );

    if f.kind().is_sequence() {
        let t = f.kind().type_name();
        quote! {
            Vec<#module::#t>
        }
    } else if f.kind().is_primitive() {
        let t = f.kind().type_name();
        quote! {
            #t
        }
    } else {
        let t = f.kind().type_name();
        quote! {
            #module::#t
        }
    }
}

#[allow(clippy::too_many_lines)]
fn body_enum(messages: &[Message], include_tag: bool) -> TokenStream {
    let variants: Vec<TokenStream> = messages
        .iter()
        .map(|message| {
            let name = message.type_name();
            let module = syn::parse_str::<syn::Path>(
                &name.to_token_stream().to_string().to_case(Case::Snake),
            )
            .unwrap();

            let dependencies: Vec<Type> = message
                .fields()
                .iter()
                .filter(|f| f.fields().is_some())
                .map(|f| f.kind().type_name())
                .chain(
                    message
                        .common_structs()
                        .unwrap_or(&[][..])
                        .iter()
                        .map(CommonStruct::type_name),
                )
                .collect();

            let pfk = pfk(
                None,
                None,
                message.fields(),
                &module,
                &dependencies,
                include_tag,
            );

            if include_tag {
                quote! {
                    #name {
                        #(#pfk,)*
                    }
                }
            } else {
                quote! {
                    #name {
                        #(#pfk,)*
                        tag_buffer: Option<crate::primitive::tagged::TagBuffer>,
                    }
                }
            }
        })
        .collect();

    if include_tag {
        let values: Vec<TokenStream> = messages
            .iter()
            .map(|message| {
                let from = syn::parse_str::<syn::Path>(&format!(
                    "mezzanine::Body::{}",
                    message.type_name().to_token_stream(),
                ))
                .unwrap();

                let from_idents = message
                    .fields()
                    .iter()
                    .filter(|field| field.tag().is_none())
                    .map(Field::ident);

                let conversions = message.fields().iter().map(|field| {
                    if field.tag().is_some() {
                        let f = field.ident();
                        let tag = field.tag().unwrap();
                        let module = syn::parse_str::<syn::Path>(&message.type_name().to_token_stream().to_string().to_case(Case::Snake))
                                    .unwrap();

                        let k = tag_kind(
                                        None,
                                        &syn::parse_str::<syn::Path>(&format!(
                                            "crate::mezzanine::{}",
                                            module.to_token_stream()
                                        ))
                                        .unwrap(),
                                        field,
                                        &[],
                                    );

                        if field.kind().is_primitive() {
                            quote! {
                                let #f = tag_buffer.as_ref().and_then(|tag_buffer| tag_buffer.decode::<#k>(&#tag).ok().unwrap_or(None))
                            }
                        } else if field.kind().is_sequence() {
                            quote! {
                                let #f = if let Some(tag_buffer) = tag_buffer.as_ref() {
                                    if let Ok(Some(#f)) =
                                    tag_buffer.decode::<#k>(&#tag)
                                    {
                                        Some(#f.into_iter().map(Into::into).collect())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            }
                        } else {
                            quote! {
                                let #f = if let Some(tag_buffer) = tag_buffer.as_ref() {
                                    if let Ok(Some(#f)) =
                                        tag_buffer.decode::<#k>(&#tag)
                                    {
                                        Some(#f.into())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            }
                        }
                    } else if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                        quote! {}
                    } else if field.kind().is_sequence() {
                        let f = field.ident();

                        quote! {
                            let #f = #f.map(|#f| #f.into_iter().map(Into::into).collect());
                        }
                    } else if field.nullable().is_some() {
                        let f = field.ident();

                        quote! {
                            let #f = #f.map(|#f|#f.into());
                        }
                    } else {
                        let f = field.ident();

                        quote! {
                            let #f  = #f.into();
                        }
                    }
                });

                let to = syn::parse_str::<syn::Path>(&format!(
                    "Self::{}",
                    message.type_name().to_token_stream(),
                ))
                .unwrap();

                let to_idents = message.fields().iter().map(Field::ident);

                if message.has_tags() {
                    quote! {
                        #from {
                            #(#from_idents,)*
                            tag_buffer,
                        } => {
                            #(#conversions;)*

                            #to {
                                #(#to_idents,)*
                            }

                        }
                    }
                } else {
                    quote! {
                        #from {
                            #(#from_idents,)*
                            tag_buffer,
                        } => {
                            #(#conversions;)*

                            let _ = tag_buffer;

                            #to {
                                #(#to_idents,)*
                            }

                        }
                    }

                }
            })
            .collect();

        quote! {
            #[derive(Clone, Debug, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            #[serde(from = "mezzanine::Body")]
            #[serde(into = "mezzanine::Body")]
            pub enum Body {
                #(#variants),*
            }

            impl From<mezzanine::Body> for Body {
                fn from(value: mezzanine::Body) -> Self {
                    match value {
                        #(#values,)*
                    }
                }
            }
        }
    } else {
        let values: Vec<TokenStream> =
            messages
                .iter()
                .map(|message| {
                    let from = syn::parse_str::<syn::Path>(&format!(
                        "crate::Body::{}",
                        message.type_name().to_token_stream(),
                    ))
                    .unwrap();

                    let from_idents = message.fields().iter().map(Field::ident);

                    let conversions = message.fields().iter().map(|field| {
                    if field.tag().is_some() {
                        let f = field.ident();
                        let tag = field.tag().unwrap();
                        let module = syn::parse_str::<syn::Path>(
                            &message.type_name().to_token_stream().to_string().to_case(Case::Snake),
                        )
                        .unwrap();
                        let k = tag_kind(
                            None,
                            &syn::parse_str::<syn::Path>(&format!(
                                "crate::mezzanine::{}",
                                module.to_token_stream()
                            ))
                            .unwrap(),
                            field,
                            &[],
                        );
                        if field.kind().is_primitive() {
                            quote! {
                                if let Some(#f) = #f {
                                    if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &#f) {
                                        tag_buffer.push(encoded);
                                    }
                                }
                            }
                        } else if field.kind().is_sequence() {
                            quote! {
                                if let Some(#f) = #f {
                                    let mezzanine: #k = #f.into_iter().map(Into::into).collect();
                                    if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                        tag_buffer.push(encoded);
                                    }
                                }
                            }
                        } else {
                            quote! {
                                if let Some(#f) = #f {
                                    let mezzanine: #k = #f.into();
                                    if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                        tag_buffer.push(encoded);
                                    }
                                }
                            }
                        }
                    } else if field.kind().is_primitive() || field.kind().is_sequence_of_primitive()
                    {
                        quote! {}
                    } else if field.kind().is_sequence() {
                        let f = field.ident();

                        quote! {
                            let #f = #f.map(|#f| #f.into_iter().map(Into::into).collect());
                        }
                    } else if field.nullable().is_some() {
                        let f = field.ident();

                        quote! {
                            let #f = #f.map(|#f|#f.into());
                        }
                    } else {
                        let f = field.ident();

                        quote! {
                            let #f  = #f.into();
                        }
                    }
                });

                    let to = syn::parse_str::<syn::Path>(&format!(
                        "Self::{}",
                        message.type_name().to_token_stream(),
                    ))
                    .unwrap();

                    let to_idents = message
                        .fields()
                        .iter()
                        .filter(|field| field.tag().is_none())
                        .map(Field::ident);

                    quote! {
                        #from {
                            #(#from_idents,)*
                        } => {
                            #[allow(unused_mut)]
                            let mut tag_buffer = Vec::new();
                            #(#conversions;)*

                            #to {
                                #(#to_idents,)*
                                tag_buffer: Some(tag_buffer.into()),
                            }
                        }
                    }
                })
                .collect();

        quote! {
            #[derive(Clone, Debug, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            pub(crate) enum Body {
                #(#variants),*
            }

            impl From<crate::Body> for Body {
                fn from(value: crate::Body) -> Self {
                    match value {
                        #(#values,)*
                    }
                }
            }
        }
    }
}

fn pfk(
    parent: Option<&Field>,
    visibility: Option<&TokenStream>,
    fields: &[Field],
    module: &syn::Path,
    dependencies: &[Type],
    include_tag: bool,
) -> Vec<TokenStream> {
    fields
        .iter()
        .filter(|field| include_tag || field.tag().is_none())
        .map(|field| {
            let f = field.ident();
            let k = kind(parent, module, field, dependencies);
            quote! {
                #visibility #f: #k
            }
        })
        .collect()
}

fn root_message_struct(
    name: &Type,
    fields: &[Field],
    common_structs: Option<&[CommonStruct]>,
    include_tag: bool,
) -> TokenStream {
    message_struct(
        &syn::parse_str::<syn::Path>(&name.to_token_stream().to_string().to_case(Case::Snake))
            .unwrap(),
        None,
        name,
        fields,
        common_structs,
        include_tag,
    )
}

#[allow(clippy::too_many_lines)]
fn message_struct(
    module: &syn::Path,
    parent: Option<&Field>,
    name: &Type,
    fields: &[Field],
    common_structs: Option<&[CommonStruct]>,
    include_tag: bool,
) -> TokenStream {
    let dependencies: Vec<Type> = fields
        .iter()
        .filter(|f| f.fields().is_some())
        .map(|f| f.kind().type_name())
        .chain(
            common_structs
                .unwrap_or(&[][..])
                .iter()
                .map(CommonStruct::type_name),
        )
        .collect();

    let token_streams: Vec<TokenStream> = fields
        .iter()
        .fold(Vec::new(), |mut acc, f| {
            if let Some(children) = f.fields().as_ref() {
                acc.push(message_struct(
                    module,
                    Some(f),
                    &f.kind().type_name(),
                    children,
                    None,
                    include_tag,
                ));
            }
            acc
        })
        .into_iter()
        .chain(
            common_structs
                .unwrap_or(&[][..])
                .iter()
                .map(|cs| common_struct(parent, module, &cs.type_name(), cs.fields(), include_tag)),
        )
        .collect();

    let vis = quote!(pub);

    let pfk = pfk(
        parent,
        Some(&vis),
        fields,
        module,
        &dependencies,
        include_tag,
    );

    if parent.is_none() {
        if token_streams.is_empty() {
            quote! {}
        } else {
            quote! {
                pub mod #module {
                    #(#token_streams)*
                }
            }
        }
    } else if include_tag {
        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!("crate::mezzanine::{}",module.to_token_stream())).unwrap(),
                    field,
                    &dependencies,
                );

                let tag = field.tag().unwrap();

                if field.kind().is_primitive() {
                    quote! {
                        let #f = value.tag_buffer.as_ref().and_then(|tag_buffer| tag_buffer.decode::<#k>(&#tag).ok().unwrap_or(None))
                    }
                } else {
                    quote! {
                        let #f = if let Some(tag_buffer) = value.tag_buffer.as_ref() {
                            if let Ok(Some(#f)) =
                                tag_buffer.decode::<#k>(&#tag)
                            {
                                Some(#f.into())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }

            })
            .collect();

        let assignments: Vec<TokenStream> = fields
            .iter()
            .map(|field| {
                let f = field.ident();

                if field.tag().is_some() {
                    quote! {
                        #f
                    }
                } else if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::mezzanine::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        quote! {
            #derived
            pub struct #name {
                #(#pfk,)*
            }

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    #(#tags;)*

                    Self {
                        #(#assignments,)*
                    }
                }
            }

            #(#token_streams)*
        }
    } else {
        #[cfg(feature = "diagnostics")]
        eprintln!(
            "mezzanine, module: {}, name: {}",
            module.to_token_stream(),
            name.to_token_stream(),
        );

        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!(
                        "crate::mezzanine::{}",
                        module.to_token_stream()
                    ))
                    .unwrap(),
                    field,
                    &dependencies,
                );

                let tag = field.tag().unwrap();

                #[cfg(feature = "diagnostics")]
                eprintln!(
                    "mezzanine, module: {}, name: {}, field: {}",
                    module.to_token_stream(),
                    name.to_token_stream(),
                    f.to_token_stream(),
                );

                if field.kind().is_primitive() {
                    quote! {
                        if let Some(#f) = value.#f {
                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &#f) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into_iter().map(Into::into).collect();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                }
            })
            .collect();

        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        quote! {
            #derived
            pub(crate) struct #name {
                #(#pfk,)*
                pub tag_buffer: Option<crate::primitive::tagged::TagBuffer>,
            }

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    #[allow(unused_mut)]
                    let mut tag_buffer = Vec::new();

                    #(#tags)*

                    Self {
                        #(#assignments,)*
                        tag_buffer: Some(tag_buffer.into()),
                    }
                }
            }

            #(#token_streams)*
        }
    }
}

#[allow(clippy::too_many_lines)]
fn common_struct(
    parent: Option<&Field>,
    module: &syn::Path,
    name: &Type,
    fields: &[Field],
    include_tag: bool,
) -> TokenStream {
    let vis = quote!(pub);
    let pfk = pfk(parent, Some(&vis), fields, module, &[], include_tag);

    if include_tag {
        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| include_tag || field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::mezzanine::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        quote! {
            #derived
            pub struct #name {
                #(#pfk,)*
            }

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    Self {
                        #(#assignments,)*
                    }
                }
            }
        }
    } else {
        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!(
                        "crate::mezzanine::{}",
                        module.to_token_stream()
                    ))
                    .unwrap(),
                    field,
                    &[],
                );

                let tag = field.tag().unwrap();

                #[cfg(feature = "diagnostics")]
                eprintln!(
                    "mezzanine, module: {}, name: {}, field: {}",
                    module.to_token_stream(),
                    name.to_token_stream(),
                    f.to_token_stream(),
                );

                if field.kind().is_primitive() {
                    quote! {
                        if let Some(#f) = value.#f {
                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &#f) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into_iter().map(Into::into).collect();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                }
            })
            .collect();

        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        quote! {
            #derived
            pub(crate) struct #name {
                #(#pfk,)*
                pub tag_buffer: Option<crate::primitive::tagged::TagBuffer>,
            }

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    #[allow(unused_mut)]
                    let mut tag_buffer = Vec::new();

                    #(#tags)*

                    Self {
                        #(#assignments,)*
                        tag_buffer: Some(tag_buffer.into()),
                    }
                }
            }

        }
    }
}

fn root(messages: &[Message], include_tag: bool) -> Vec<TokenStream> {
    messages
        .iter()
        .map(|message| {
            root_message_struct(
                &message.type_name(),
                message.fields(),
                message.common_structs(),
                include_tag,
            )
        })
        .collect()
}

fn process(messages: &[Message], include_tag: bool) -> TokenStream {
    let body_enum = body_enum(messages, include_tag);
    let root = root(messages, include_tag);

    if include_tag {
        quote! {
            #(#root)*

            #body_enum
        }
    } else {
        quote! {
            mod mezzanine {
                #(#root)*

                #body_enum
            }
        }
    }
}

fn all(pattern: &str) -> Result<Vec<Message>> {
    glob::glob(pattern)
        .map_err(Into::into)
        .and_then(|mut paths| {
            paths.try_fold(Vec::new(), |mut acc, p| {
                p.map_err(Into::into)
                    .inspect(|path| println!("cargo::rerun-if-changed={}", path.display()))
                    .and_then(read_value)
                    .and_then(|v| Message::try_from(&Wv::from(&v)).map_err(Into::into))
                    .map(|m| {
                        acc.push(m);
                        acc
                    })
            })
        })
}

fn each_field_meta(
    field: &Field,
    common_structs: &Option<HashMap<Type, &CommonStruct>>,
) -> TokenStream {
    let name = field.name().to_case(Case::Snake);
    let version = field.versions();
    let nullable = OptionWrapper::from(field.nullable());
    let kind = field.kind().name();
    let tag = OptionWrapper::from(field.tag());
    let tagged = OptionWrapper::from(field.tagged());

    let children = field.fields().as_ref().map_or_else(
        || {
            common_structs.as_ref().map_or(Vec::new(), |m| {
                m.get(&field.kind().type_name()).map_or(Vec::new(), |cs| {
                    cs.fields()
                        .iter()
                        .map(|f| each_field_meta(f, common_structs))
                        .collect()
                })
            })
        },
        |fields| {
            fields
                .iter()
                .map(|f| each_field_meta(f, common_structs))
                .collect()
        },
    );

    quote! {
        (#name,
        &tansu_kafka_model::FieldMeta {
            version: #version,
            nullable: #nullable,
            kind: tansu_kafka_model::KindMeta(#kind),
            tag: #tag,
            tagged: #tagged,
            fields: &[#(#children),*],
        })
    }
}

fn each_message_meta(message: &Message) -> TokenStream {
    let name = message.name();
    let api_key = message.api_key();
    let version = message.version();
    let message_kind = message.kind();

    let common_structs = message
        .common_structs()
        .as_ref()
        .map(|v| v.iter().map(|cs| (cs.type_name(), cs)))
        .map(HashMap::from_iter);

    let children = message
        .fields()
        .iter()
        .map(|f| each_field_meta(f, &common_structs));

    quote! {
        (#name,
        &tansu_kafka_model::MessageMeta {
            name: #name,
            api_key: #api_key,
            version: #version,
            message_kind: #message_kind,
            fields: &[#(#children),*],
        })
    }
}

fn message_meta(messages: &[Message]) -> TokenStream {
    let len = messages.len();

    let meta = messages.iter().map(each_message_meta);

    quote! {
        pub static MESSAGE_META : [(&str, &tansu_kafka_model::MessageMeta); #len] = [#(#meta, )*];
    }
}

pub fn main() {
    let files = "message/[A-Z]*Re[qs]*.json";

    let messages = all(files).unwrap_or_else(|e| panic!("all: {e:?}"));

    let tagged = process(&messages, true);
    let untagged = process(&messages, false);

    let message_meta = message_meta(&messages);

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generate.rs");

    let q = quote! {
        #tagged
        #untagged
        #message_meta
    };

    let r = syn::parse_file(&q.to_string()).unwrap();

    fs::write(&dest_path, prettyplease::unparse(&r)).unwrap();

    println!("cargo::rerun-if-changed=build.rs");
}

struct OptionWrapper<T>(Option<T>);

impl<T> From<Option<T>> for OptionWrapper<T> {
    fn from(value: Option<T>) -> Self {
        Self(value)
    }
}

impl<T: ToTokens> ToTokens for OptionWrapper<T> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        if let Some(ref v) = self.0 {
            tokens.extend(quote! {
                Some(#v)
            });
        } else {
            tokens.extend(quote! {
                None
            });
        }
    }
}
