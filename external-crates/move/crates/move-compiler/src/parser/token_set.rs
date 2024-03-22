// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::parser::lexer::{Tok, TOK_COUNT};

use move_symbol_pool::Symbol;

use once_cell::sync::Lazy;
use std::collections::HashMap;

use super::ast::{ENTRY_MODIFIER, MACRO_MODIFIER, NATIVE_MODIFIER};

#[derive(Clone, Debug)]
pub struct TokenSet {
    tokens: [u8; TOK_COUNT],
    identifiers: HashMap<Symbol, u8>,
}

//**************************************************************************************************
// CONSTANT SETS
//**************************************************************************************************

const MODULE_MEMBER_TOKENS: [Tok; 7] = [
    Tok::Fun,
    Tok::Struct,
    Tok::Use,
    Tok::Const,
    Tok::Friend,
    Tok::Spec,
    Tok::Invariant,
];

const MEMBER_VISIBILITY_TOKENS: &[Tok] = &[Tok::Public];

const MEMBER_MODIFIER_TOKENS: [Tok; 1] = [Tok::Native];

pub static MODULE_MEMBER_OR_MODULE_START_SET: Lazy<TokenSet> = Lazy::new(|| {
    let mut token_set = TokenSet::new();
    token_set.add_all(&MODULE_MEMBER_TOKENS);
    token_set.add_all(MEMBER_VISIBILITY_TOKENS);
    token_set.add_all(&MEMBER_MODIFIER_TOKENS);
    token_set.add_identifier(MACRO_MODIFIER);
    token_set.add_identifier(ENTRY_MODIFIER);
    token_set.add_identifier(NATIVE_MODIFIER);
    token_set.add(Tok::Module);
    // both a member and module can be annotated
    token_set.add(Tok::NumSign);
    token_set
});

const PARAM_STARTS: [Tok; 5] = [
    Tok::Identifier,
    Tok::Mut,
    Tok::SyntaxIdentifier,
    Tok::LParen,
    Tok::RestrictedIdentifier,
];

pub static PARAM_START_SET: Lazy<TokenSet> = Lazy::new(|| TokenSet::from(&PARAM_STARTS));

const EXP_STARTS: [Tok; 27] = [
    Tok::NumValue,
    Tok::NumTypedValue,
    Tok::ByteStringValue,
    Tok::Identifier,
    Tok::SyntaxIdentifier,
    Tok::RestrictedIdentifier,
    Tok::AtSign,
    Tok::Copy,
    Tok::Move,
    Tok::Pipe,
    Tok::PipePipe,
    Tok::False,
    Tok::True,
    Tok::Amp,
    Tok::AmpMut,
    Tok::Star,
    Tok::Exclaim,
    Tok::LParen,
    Tok::LBrace,
    Tok::Abort,
    Tok::Break,
    Tok::Continue,
    Tok::If,
    Tok::Loop,
    Tok::Return,
    Tok::While,
    Tok::BlockLabel,
];

pub static EXP_START_SET: Lazy<TokenSet> = Lazy::new(|| TokenSet::from(&EXP_STARTS));

const TYPE_STARTS: [Tok; 9] = [
    Tok::Identifier,
    Tok::Amp,
    Tok::AmpMut,
    Tok::LParen,   // tuple
    Tok::NumValue, // package address
    Tok::Pipe,
    Tok::PipePipe,
    Tok::SyntaxIdentifier,
    Tok::RestrictedIdentifier,
];

pub static TYPE_START_SET: Lazy<TokenSet> = Lazy::new(|| TokenSet::from(&TYPE_STARTS));

// including `Tok::For` here is hack for `#[syntax(for)]` attribute (similar to the one in
// `syntax::parse_attribute`)
const ATTR_STARTS: [Tok; 2] = [Tok::Identifier, Tok::For];

pub static ATTR_START_SET: Lazy<TokenSet> = Lazy::new(|| TokenSet::from(&ATTR_STARTS));

const FIELD_BINDING_STARTS: [Tok; 3] = [Tok::Mut, Tok::Identifier, Tok::RestrictedIdentifier];

pub static FIELD_BINDING_START_SET: Lazy<TokenSet> =
    Lazy::new(|| TokenSet::from(&FIELD_BINDING_STARTS));

//**************************************************************************************************
// IMPLS
//**************************************************************************************************

#[allow(dead_code)]
impl TokenSet {
    pub fn new() -> Self {
        let tokens = [0; TOK_COUNT];
        let identifiers = HashMap::new();
        TokenSet {
            tokens,
            identifiers,
        }
    }

    pub fn add(&mut self, tok: Tok) {
        self.tokens[tok as usize] += 1;
    }

    pub fn remove(&mut self, tok: Tok) {
        if self.tokens[tok as usize] > 0 {
            self.tokens[tok as usize] -= 1;
        }
    }

    pub fn add_identifier(&mut self, identifier: &str) {
        *self.identifiers.entry(identifier.into()).or_default() += 1;
    }

    pub fn remove_identifier(&mut self, identifier: impl AsRef<str>) {
        if let Some(entry) = self.identifiers.get_mut(&identifier.as_ref().into()) {
            if *entry < 2 {
                self.identifiers.remove(&identifier.as_ref().into());
            } else {
                *entry -= 1;
            }
        }
    }

    pub fn add_all(&mut self, toks: &[Tok]) {
        for tok in toks {
            self.add(*tok);
        }
    }

    pub fn remove_all(&mut self, toks: &[Tok]) {
        for tok in toks {
            self.remove(*tok);
        }
    }

    pub fn contains(&self, tok: Tok, tok_contents: impl AsRef<str>) -> bool {
        self.tokens[tok as usize] > 0
            || (tok == Tok::Identifier
                || tok == Tok::RestrictedIdentifier
                || tok == Tok::SyntaxIdentifier)
                && self.identifiers.contains_key(&tok_contents.as_ref().into())
    }

    pub fn contains_any(&self, toks: &[Tok], tok_contents: impl AsRef<str>) -> bool {
        toks.iter()
            .any(|tok| self.contains(*tok, tok_contents.as_ref()))
    }

    pub fn union(&mut self, other: &TokenSet) {
        for (target, n) in self.tokens.iter_mut().zip(other.tokens.iter()) {
            *target += n;
        }
        for (identifier, n) in other.identifiers.iter() {
            *self.identifiers.entry(*identifier).or_default() += n;
        }
    }

    pub fn difference(&mut self, other: &TokenSet) {
        for (target, n) in self.tokens.iter_mut().zip(other.tokens.iter()) {
            if *target >= *n {
                *target -= n;
            } else {
                *target = 0
            }
        }
        for (identifier, n) in other.identifiers.iter() {
            let entry = self.identifiers.entry(*identifier).or_default();
            if *entry >= *n {
                *entry -= n;
            } else {
                *entry = 0
            }
        }
    }
}

impl<const N: usize> std::convert::From<[Tok; N]> for TokenSet {
    fn from(values: [Tok; N]) -> Self {
        let mut new = TokenSet::new();
        new.add_all(&values);
        new
    }
}

impl<const N: usize> std::convert::From<&[Tok; N]> for TokenSet {
    fn from(values: &[Tok; N]) -> Self {
        let mut new = TokenSet::new();
        new.add_all(values);
        new
    }
}

impl std::convert::From<&[Tok]> for TokenSet {
    fn from(values: &[Tok]) -> Self {
        let mut new = TokenSet::new();
        new.add_all(values);
        new
    }
}
