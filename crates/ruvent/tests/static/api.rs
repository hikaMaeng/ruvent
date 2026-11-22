//! The public surface, frozen.
//!
//! Every `pub` item of the library — functions with their full signatures,
//! struct fields, enum variants, trait methods, re-exports — is rendered to
//! one line and compared with the committed `api.txt`. A change to the
//! surface fails here until someone looks at the difference and decides it.
//!
//! `RUVENT_BLESS=1` rewrites the file. Review the diff before committing it:
//! a line that disappears is a break for every application built on this.

use crate::source::{crate_root, library};
use quote::ToTokens;
use std::collections::HashSet;
use syn::{Attribute, Fields, ImplItem, Item, TraitItem, Visibility};

fn public(visibility: &Visibility) -> bool {
    matches!(visibility, Visibility::Public(_))
}

fn test_only(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        let text = attribute.to_token_stream().to_string();
        text.contains("cfg (test)") || text.contains("cfg (doctest)")
    })
}

fn tokens(item: &impl ToTokens) -> String {
    item.to_token_stream().to_string()
}

/// `every` is for enum variants, whose fields are public without saying so.
fn fields(fields: &Fields, every: bool) -> String {
    match fields {
        Fields::Named(named) => {
            let shown: Vec<String> = named.named.iter().filter(|field| every || public(&field.vis)).map(|field| format!("{}: {}", field.ident.as_ref().unwrap(), tokens(&field.ty))).collect();
            format!(" {{ {} }}", shown.join(", "))
        }
        Fields::Unnamed(unnamed) => {
            let shown: Vec<String> = unnamed.unnamed.iter().map(|field| if every || public(&field.vis) { tokens(&field.ty) } else { "_".to_owned() }).collect();
            format!("({})", shown.join(", "))
        }
        Fields::Unit => String::new(),
    }
}

fn collect(path: &str, items: &[Item], hidden: &HashSet<String>, out: &mut Vec<String>) {
    for item in items {
        match item {
            Item::Fn(function) if public(&function.vis) && !test_only(&function.attrs) => out.push(format!("{path}: {}", tokens(&function.sig))),
            Item::Struct(structure) if public(&structure.vis) && !test_only(&structure.attrs) => {
                out.push(format!("{path}: struct {}{}{}", structure.ident, tokens(&structure.generics), fields(&structure.fields, false)));
            }
            Item::Enum(enumeration) if public(&enumeration.vis) && !test_only(&enumeration.attrs) => {
                let variants: Vec<String> = enumeration.variants.iter().map(|variant| format!("{}{}", variant.ident, fields(&variant.fields, true))).collect();
                out.push(format!("{path}: enum {}{} {{ {} }}", enumeration.ident, tokens(&enumeration.generics), variants.join(", ")));
            }
            Item::Trait(definition) if public(&definition.vis) && !test_only(&definition.attrs) => {
                out.push(format!("{path}: trait {}{} : {}", definition.ident, tokens(&definition.generics), tokens(&definition.supertraits)));
                for member in &definition.items {
                    if let TraitItem::Fn(method) = member {
                        out.push(format!("{path}: trait {} :: {}", definition.ident, tokens(&method.sig)));
                    }
                }
            }
            Item::Type(alias) if public(&alias.vis) => out.push(format!("{path}: type {} = {}", alias.ident, tokens(&alias.ty))),
            Item::Const(constant) if public(&constant.vis) => out.push(format!("{path}: const {}: {}", constant.ident, tokens(&constant.ty))),
            Item::Use(declaration) if public(&declaration.vis) => out.push(format!("{path}: use {}", tokens(&declaration.tree))),
            Item::Mod(module) if public(&module.vis) || module.content.is_some() => {
                if test_only(&module.attrs) {
                    continue;
                }
                if let Some((_, inner)) = &module.content {
                    collect(&format!("{path}::{}", module.ident), inner, hidden, out);
                } else if public(&module.vis) {
                    out.push(format!("{path}: pub mod {}", module.ident));
                }
            }
            Item::Impl(block) if !test_only(&block.attrs) => {
                // Methods of a type nobody outside the crate can name are not surface.
                if let syn::Type::Path(named) = &*block.self_ty {
                    if named.path.segments.last().is_some_and(|last| hidden.contains(&last.ident.to_string())) {
                        continue;
                    }
                }
                let generics = format!("{}{}", tokens(&block.generics), block.generics.where_clause.as_ref().map(|clause| format!(" {}", tokens(clause))).unwrap_or_default());
                let subject = match &block.trait_ {
                    Some((_, name, _)) => format!("impl{generics} {} for {}", tokens(name), tokens(&block.self_ty)),
                    None => format!("impl{generics} {}", tokens(&block.self_ty)),
                };
                if block.trait_.is_some() {
                    // A trait implementation is surface whether or not its
                    // methods say `pub`: removing one breaks callers.
                    out.push(format!("{path}: {subject}"));
                    continue;
                }
                for member in &block.items {
                    if let ImplItem::Fn(method) = member {
                        if public(&method.vis) && !test_only(&method.attrs) {
                            out.push(format!("{path}: {subject} :: {}", tokens(&method.sig)));
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

/// Types declared without `pub`: their impl blocks are internal.
fn hidden(files: &[syn::File]) -> HashSet<String> {
    let mut names = HashSet::new();
    for file in files {
        for item in &file.items {
            match item {
                Item::Struct(structure) if !public(&structure.vis) => drop(names.insert(structure.ident.to_string())),
                Item::Enum(enumeration) if !public(&enumeration.vis) => drop(names.insert(enumeration.ident.to_string())),
                _ => {}
            }
        }
    }
    names
}

fn surface() -> String {
    let sources = library();
    let files: Vec<syn::File> = sources.iter().map(|source| syn::parse_file(&source.text).unwrap_or_else(|error| panic!("{}: {error}", source.path))).collect();
    let hidden = hidden(&files);
    let mut lines = Vec::new();
    for (source, file) in sources.iter().zip(&files) {
        collect(&source.path, &file.items, &hidden, &mut lines);
    }
    lines.sort();
    lines.join("\n") + "\n"
}

#[test]
fn the_public_api_is_the_committed_one() {
    let path = crate_root().join("tests/static/api.txt");
    let now = surface();
    if std::env::var_os("RUVENT_BLESS").is_some() {
        std::fs::write(&path, &now).unwrap();
    }
    let committed = std::fs::read_to_string(&path).expect("api.txt missing; run once with RUVENT_BLESS=1").replace("\r\n", "\n");
    if now != committed {
        let old: std::collections::BTreeSet<&str> = committed.lines().collect();
        let new: std::collections::BTreeSet<&str> = now.lines().collect();
        let removed: Vec<&&str> = old.difference(&new).collect();
        let added: Vec<&&str> = new.difference(&old).collect();
        panic!("the public API changed.\nremoved:\n{removed:#?}\nadded:\n{added:#?}\nIf this is intended, rerun with RUVENT_BLESS=1 and review the diff of tests/static/api.txt.");
    }
}
