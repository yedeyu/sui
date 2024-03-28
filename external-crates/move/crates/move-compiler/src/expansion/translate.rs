// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    diag,
    diagnostics::{codes::WarningFilter, Diagnostic, WarningFilters},
    editions::{self, create_feature_error, Edition, FeatureGate, Flavor},
    expansion::{
        alias_map_builder::{
            AliasEntry, AliasMapBuilder, NameSpace, ParserExplicitUseFun, UnnecessaryAlias,
            UseFunsBuilder,
        },
        aliases::{AliasMap, AliasSet},
        ast::{self as E, Address, Fields, ModuleIdent, ModuleIdent_},
        byte_string, hex_string, legacy_aliases,
        translate::known_attributes::{DiagnosticAttribute, KnownAttribute},
    },
    ice,
    parser::ast::{
        self as P, Ability, BlockLabel, ConstantName, Field, FieldBindings, FunctionName,
        ModuleName, StructName, Var, ENTRY_MODIFIER, MACRO_MODIFIER, NATIVE_MODIFIER,
    },
    shared::{known_attributes::AttributePosition, unique_map::UniqueMap, *},
    FullyCompiledProgram,
};
use move_command_line_common::parser::{parse_u16, parse_u256, parse_u32};
use move_core_types::account_address::AccountAddress;
use move_ir_types::location::*;
use move_proc_macros::growing_stack;
use move_symbol_pool::Symbol;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    iter::IntoIterator,
    sync::Arc,
};

//**************************************************************************************************
// Context
//**************************************************************************************************

type ModuleMembers = BTreeMap<Name, ModuleMemberKind>;

// NB: We carry a few things separately because we need to split them out during path resolution to
// allow for dynamic behavior during that resolution. This dynamic behavior allows us to reuse the
// majority of the pass while swapping out how we handle paths and aliases for Move 2024 versus
// legacy.

struct DefnContext<'env, 'map> {
    named_address_mapping: Option<&'map NamedAddressMap>,
    module_members: UniqueMap<ModuleIdent, ModuleMembers>,
    env: &'env mut CompilationEnv,
    address_conflicts: BTreeSet<Symbol>,
    current_package: Option<Symbol>,
}

struct Context<'env, 'map> {
    defn_context: DefnContext<'env, 'map>,
    address: Option<Address>,
    is_source_definition: bool,
    // Cached warning filters for all available prefixes. Used by non-source defs
    // and dependency packages
    all_filter_alls: WarningFilters,
    pub path_expander: Option<Box<dyn PathExpander>>,
}

impl<'env, 'map> Context<'env, 'map> {
    fn new(
        compilation_env: &'env mut CompilationEnv,
        module_members: UniqueMap<ModuleIdent, ModuleMembers>,
        address_conflicts: BTreeSet<Symbol>,
    ) -> Self {
        let mut all_filter_alls = WarningFilters::new_for_dependency();
        for prefix in compilation_env.known_filter_names() {
            for f in compilation_env.filter_from_str(prefix, FILTER_ALL) {
                all_filter_alls.add(f);
            }
        }
        let defn_context = DefnContext {
            env: compilation_env,
            named_address_mapping: None,
            address_conflicts,
            module_members,
            current_package: None,
        };
        Context {
            defn_context,
            address: None,
            is_source_definition: false,
            all_filter_alls,
            path_expander: None,
        }
    }

    fn env(&mut self) -> &mut CompilationEnv {
        self.defn_context.env
    }

    fn current_package(&mut self) -> Option<Symbol> {
        self.defn_context.current_package
    }

    fn cur_address(&self) -> &Address {
        self.address.as_ref().unwrap()
    }

    pub fn new_alias_map_builder(&mut self) -> AliasMapBuilder {
        let current_package = self.current_package();
        let new_paths = self
            .defn_context
            .env
            .supports_feature(current_package, FeatureGate::Move2024Paths);
        if new_paths {
            AliasMapBuilder::namespaced()
        } else {
            AliasMapBuilder::legacy()
        }
    }

    /// Pushes a new alias map onto the alias information in the pash expander.
    pub fn push_alias_scope(&mut self, loc: Loc, new_scope: AliasMapBuilder) {
        let res = self
            .path_expander
            .as_mut()
            .unwrap()
            .push_alias_scope(loc, new_scope);
        match res {
            Err(diag) => self.env().add_diag(*diag),
            Ok(unnecessaries) => unnecessary_alias_errors(self, unnecessaries),
        }
    }

    // Push a number of type parameters onto the alias information in the path expander.
    pub fn push_type_parameters<'a, I: IntoIterator<Item = &'a Name>>(&mut self, tparams: I)
    where
        I::IntoIter: ExactSizeIterator,
    {
        self.path_expander
            .as_mut()
            .unwrap()
            .push_type_parameters(tparams.into_iter().collect::<Vec<_>>());
    }

    /// Pops the innermost alias information on the path expander and reports errors for aliases
    /// that were unused Marks implicit use funs as unused
    pub fn pop_alias_scope(&mut self, mut use_funs: Option<&mut E::UseFuns>) {
        let AliasSet { modules, members } = self.path_expander.as_mut().unwrap().pop_alias_scope();
        for alias in modules {
            unused_alias(self, "module", alias)
        }
        for alias in members {
            let use_fun_used_opt = use_funs
                .as_mut()
                .and_then(|use_funs| use_funs.implicit.get_mut(&alias))
                .and_then(|use_fun| match &mut use_fun.kind {
                    E::ImplicitUseFunKind::FunctionDeclaration => None,
                    E::ImplicitUseFunKind::UseAlias { used } => Some(used),
                });
            if let Some(used) = use_fun_used_opt {
                // We do not report the use error if it is a function alias, since these will be
                // reported after method calls are fully resolved
                *used = false;
            } else {
                unused_alias(self, "member", alias)
            }
        }
    }

    pub fn attribute_value(
        &mut self,
        attribute_value: P::AttributeValue,
    ) -> Option<E::AttributeValue> {
        let Context {
            path_expander,
            defn_context: inner_context,
            ..
        } = self;
        path_expander
            .as_mut()
            .unwrap()
            .name_access_chain_to_attribute_value(inner_context, attribute_value)
    }

    pub fn name_access_chain_to_module_access(
        &mut self,
        access: Access,
        chain: P::NameAccessChain,
    ) -> Option<E::ModuleAccess> {
        let Context {
            path_expander,
            defn_context: inner_context,
            ..
        } = self;
        path_expander
            .as_mut()
            .unwrap()
            .name_access_chain_to_module_access(inner_context, access, chain)
    }

    pub fn name_access_chain_to_module_ident(
        &mut self,
        chain: P::NameAccessChain,
    ) -> Option<E::ModuleIdent> {
        let Context {
            path_expander,
            defn_context: inner_context,
            ..
        } = self;
        path_expander
            .as_mut()
            .unwrap()
            .name_access_chain_to_module_ident(inner_context, chain)
    }

    pub fn spec_deprecated(&mut self, loc: Loc, is_error: bool) {
        let diag = self.spec_deprecated_diag(loc, is_error);
        self.env().add_diag(diag);
    }

    pub fn spec_deprecated_diag(&mut self, loc: Loc, is_error: bool) -> Diagnostic {
        diag!(
            if is_error {
                Uncategorized::DeprecatedSpecItem
            } else {
                Uncategorized::DeprecatedWillBeRemoved
            },
            (
                loc,
                "Specification blocks are deprecated and are no longer used"
            )
        )
    }
}

fn unnecessary_alias_errors(context: &mut Context, unnecessaries: Vec<UnnecessaryAlias>) {
    for unnecessary in unnecessaries {
        unnecessary_alias_error(context, unnecessary)
    }
}

fn unnecessary_alias_error(context: &mut Context, unnecessary: UnnecessaryAlias) {
    let UnnecessaryAlias { entry, prev } = unnecessary;
    let loc = entry.loc();
    let is_default = prev == Loc::invalid();
    let (alias, entry_case) = match entry {
        AliasEntry::Address(_, _) => {
            debug_assert!(false, "ICE cannot manually make address aliases");
            return;
        }
        AliasEntry::TypeParam(_) => {
            debug_assert!(
                false,
                "ICE cannot manually make type param aliases. \
                We do not have nested TypeParam scopes"
            );
            return;
        }
        AliasEntry::Module(n, m) => (n, format!(" for module '{m}'")),
        AliasEntry::Member(n, m, mem) => (n, format!(" for module member '{m}::{mem}'")),
    };
    let decl_case = if is_default {
        "This alias is provided by default"
    } else {
        "It was already in scope"
    };
    let msg = format!("Unnecessary alias '{alias}'{entry_case}. {decl_case}");
    let mut diag = diag!(Declarations::DuplicateAlias, (loc, msg));
    if prev != Loc::invalid() {
        // nothing to point to for the default case
        diag.add_secondary_label((prev, "The same alias was previously declared here"))
    }
    context.env().add_diag(diag);
}

/// We mark named addresses as having a conflict if there is not a bidirectional mapping between
/// the name and its value
fn compute_address_conflicts(
    pre_compiled_lib: Option<Arc<FullyCompiledProgram>>,
    prog: &P::Program,
) -> BTreeSet<Symbol> {
    let mut name_to_addr: BTreeMap<Symbol, BTreeSet<AccountAddress>> = BTreeMap::new();
    let mut addr_to_name: BTreeMap<AccountAddress, BTreeSet<Symbol>> = BTreeMap::new();
    let all_addrs = prog.named_address_maps.all().iter().chain(
        pre_compiled_lib
            .iter()
            .flat_map(|pre| pre.parser.named_address_maps.all()),
    );
    for map in all_addrs {
        for (n, addr) in map {
            let n = *n;
            let addr = addr.into_inner();
            name_to_addr.entry(n).or_default().insert(addr);
            addr_to_name.entry(addr).or_default().insert(n);
        }
    }
    let name_to_addr_conflicts = name_to_addr
        .into_iter()
        .filter(|(_, addrs)| addrs.len() > 1)
        .map(|(n, _)| n);
    let addr_to_name_conflicts = addr_to_name
        .into_iter()
        .filter(|(_, addrs)| addrs.len() > 1)
        .flat_map(|(_, ns)| ns.into_iter());
    name_to_addr_conflicts
        .chain(addr_to_name_conflicts)
        .collect()
}

// Implicit aliases for the Move Stdlib:
// use std::vector;
// use std::option::{Self, Option};
const IMPLICIT_STD_MODULES: &[Symbol] = &[symbol!("option"), symbol!("vector")];
const IMPLICIT_STD_MEMBERS: &[(Symbol, Symbol, ModuleMemberKind)] = &[(
    symbol!("option"),
    symbol!("Option"),
    ModuleMemberKind::Struct,
)];

// Implicit aliases for Sui mode:
// use sui::object::{Self, ID, UID};
// use sui::transfer;
// use sui::tx_context::{Self, TxContext};
const IMPLICIT_SUI_MODULES: &[Symbol] = &[
    symbol!("object"),
    symbol!("transfer"),
    symbol!("tx_context"),
];
const IMPLICIT_SUI_MEMBERS: &[(Symbol, Symbol, ModuleMemberKind)] = &[
    (symbol!("object"), symbol!("ID"), ModuleMemberKind::Struct),
    (symbol!("object"), symbol!("UID"), ModuleMemberKind::Struct),
    (
        symbol!("tx_context"),
        symbol!("TxContext"),
        ModuleMemberKind::Struct,
    ),
];

fn default_aliases(context: &mut Context) -> AliasMapBuilder {
    let current_package = context.current_package();
    let mut builder = context.new_alias_map_builder();
    if !context
        .env()
        .supports_feature(current_package, FeatureGate::Move2024Paths)
    {
        return builder;
    }
    // Unused loc since these will not conflict and are implicit so no warnings are given
    let loc = Loc::invalid();
    let std_address = maybe_make_well_known_address(context, loc, symbol!("std"));
    let sui_address = maybe_make_well_known_address(context, loc, symbol!("sui"));
    let mut modules: Vec<(Address, Symbol)> = vec![];
    let mut members: Vec<(Address, Symbol, Symbol, ModuleMemberKind)> = vec![];
    // if std is defined, add implicit std aliases
    if let Some(std_address) = std_address {
        modules.extend(
            IMPLICIT_STD_MODULES
                .iter()
                .copied()
                .map(|m| (std_address, m)),
        );
        members.extend(
            IMPLICIT_STD_MEMBERS
                .iter()
                .copied()
                .map(|(m, mem, k)| (std_address, m, mem, k)),
        );
    }
    // if sui is defined and the current package is in Sui mode, add implicit sui aliases
    if sui_address.is_some() && context.env().package_config(current_package).flavor == Flavor::Sui
    {
        let sui_address = sui_address.unwrap();
        modules.extend(
            IMPLICIT_SUI_MODULES
                .iter()
                .copied()
                .map(|m| (sui_address, m)),
        );
        members.extend(
            IMPLICIT_SUI_MEMBERS
                .iter()
                .copied()
                .map(|(m, mem, k)| (sui_address, m, mem, k)),
        );
    }
    for (addr, module) in modules {
        let alias = sp(loc, module);
        let mident = sp(loc, ModuleIdent_::new(addr, ModuleName(sp(loc, module))));
        builder.add_implicit_module_alias(alias, mident).unwrap();
    }
    for (addr, module, member, kind) in members {
        let alias = sp(loc, member);
        let mident = sp(loc, ModuleIdent_::new(addr, ModuleName(sp(loc, module))));
        let name = sp(loc, member);
        builder
            .add_implicit_member_alias(alias, mident, name, kind)
            .unwrap();
    }
    builder
}

//**************************************************************************************************
// Entry
//**************************************************************************************************

pub fn program(
    compilation_env: &mut CompilationEnv,
    pre_compiled_lib: Option<Arc<FullyCompiledProgram>>,
    prog: P::Program,
) -> E::Program {
    let address_conflicts = compute_address_conflicts(pre_compiled_lib.clone(), &prog);

    let mut member_computation_context = DefnContext {
        env: compilation_env,
        named_address_mapping: None,
        module_members: UniqueMap::new(),
        address_conflicts,
        current_package: None,
    };

    let module_members = {
        let mut members = UniqueMap::new();
        all_module_members(
            &mut member_computation_context,
            &prog.named_address_maps,
            &mut members,
            true,
            &prog.source_definitions,
        );
        all_module_members(
            &mut member_computation_context,
            &prog.named_address_maps,
            &mut members,
            true,
            &prog.lib_definitions,
        );
        if let Some(pre_compiled) = pre_compiled_lib.clone() {
            assert!(pre_compiled.parser.lib_definitions.is_empty());
            all_module_members(
                &mut member_computation_context,
                &pre_compiled.parser.named_address_maps,
                &mut members,
                false,
                &pre_compiled.parser.source_definitions,
            );
        }
        members
    };

    let address_conflicts = member_computation_context.address_conflicts;

    let mut source_module_map = UniqueMap::new();
    let mut lib_module_map = UniqueMap::new();
    let P::Program {
        named_address_maps,
        source_definitions,
        lib_definitions,
    } = prog;

    let mut context = Context::new(compilation_env, module_members, address_conflicts);

    context.is_source_definition = true;
    for P::PackageDefinition {
        package,
        named_address_map,
        def,
    } in source_definitions
    {
        context.defn_context.current_package = package;
        let named_address_map = named_address_maps.get(named_address_map);
        if context
            .env()
            .supports_feature(package, FeatureGate::Move2024Paths)
        {
            let mut path_expander = Move2024PathExpander::new();

            let aliases = named_addr_map_to_alias_map_builder(&mut context, named_address_map);

            // should never fail
            if let Err(diag) = path_expander.push_alias_scope(Loc::invalid(), aliases) {
                context.env().add_diag(*diag);
            }

            context.defn_context.named_address_mapping = Some(named_address_map);
            context.path_expander = Some(Box::new(path_expander));
            definition(&mut context, &mut source_module_map, package, def);
            context.pop_alias_scope(None); // Handle unused addresses in this case
            context.path_expander = None;
        } else {
            context.defn_context.named_address_mapping = Some(named_address_map);
            context.path_expander = Some(Box::new(LegacyPathExpander::new()));
            definition(&mut context, &mut source_module_map, package, def);
            context.path_expander = None;
        }
    }

    context.is_source_definition = false;
    for P::PackageDefinition {
        package,
        named_address_map,
        def,
    } in lib_definitions
    {
        context.defn_context.current_package = package;
        let named_address_map = named_address_maps.get(named_address_map);
        if context
            .env()
            .supports_feature(package, FeatureGate::Move2024Paths)
        {
            let mut path_expander = Move2024PathExpander::new();

            let aliases = named_addr_map_to_alias_map_builder(&mut context, named_address_map);
            // should never fail
            if let Err(diag) = path_expander.push_alias_scope(Loc::invalid(), aliases) {
                context.env().add_diag(*diag);
            }
            context.defn_context.named_address_mapping = Some(named_address_map);
            context.path_expander = Some(Box::new(path_expander));
            definition(&mut context, &mut lib_module_map, package, def);
            context.pop_alias_scope(None); // Handle unused addresses in this case
            context.path_expander = None;
        } else {
            context.defn_context.named_address_mapping = Some(named_address_map);
            context.path_expander = Some(Box::new(LegacyPathExpander::new()));
            definition(&mut context, &mut lib_module_map, package, def);
            context.path_expander = None;
        }
    }

    context.defn_context.current_package = None;

    // Finalization
    //
    for (mident, module) in lib_module_map {
        if let Err((mident, old_loc)) = source_module_map.add(mident, module) {
            if !context.env().flags().sources_shadow_deps() {
                duplicate_module(&mut context, &source_module_map, mident, old_loc)
            }
        }
    }
    let module_map = source_module_map;

    super::primitive_definers::modules(context.env(), pre_compiled_lib, &module_map);
    E::Program {
        modules: module_map,
    }
}

fn definition(
    context: &mut Context,
    module_map: &mut UniqueMap<ModuleIdent, E::ModuleDefinition>,
    package_name: Option<Symbol>,
    def: P::Definition,
) {
    let default_aliases = default_aliases(context);
    context.push_alias_scope(/* unused */ Loc::invalid(), default_aliases);
    match def {
        P::Definition::Module(mut m) => {
            let module_paddr = std::mem::take(&mut m.address);
            let module_addr = module_paddr.map(|addr| {
                let address = top_level_address(
                    &mut context.defn_context,
                    /* suggest_declaration */ true,
                    addr,
                );
                sp(addr.loc, address)
            });
            module(context, module_map, package_name, module_addr, m)
        }
        P::Definition::Address(a) => {
            let addr = top_level_address(
                &mut context.defn_context,
                /* suggest_declaration */ false,
                a.addr,
            );
            for mut m in a.modules {
                let module_addr = check_module_address(context, a.loc, addr, &mut m);
                module(context, module_map, package_name, Some(module_addr), m)
            }
        }
    }
    context.pop_alias_scope(None);
}

// Access a top level address as declared, not affected by any aliasing/shadowing
fn top_level_address(
    context: &mut DefnContext,
    suggest_declaration: bool,
    ln: P::LeadingNameAccess,
) -> Address {
    top_level_address_(
        context,
        context.named_address_mapping.as_ref().unwrap(),
        suggest_declaration,
        ln,
    )
}

fn top_level_address_(
    context: &mut DefnContext,
    named_address_mapping: &NamedAddressMap,
    suggest_declaration: bool,
    ln: P::LeadingNameAccess,
) -> Address {
    let name_res = check_valid_address_name(context, &ln);
    let sp!(loc, ln_) = ln;
    match ln_ {
        P::LeadingNameAccess_::AnonymousAddress(bytes) => {
            debug_assert!(name_res.is_ok());
            Address::anonymous(loc, bytes)
        }
        // This should have been handled elsewhere in alias resolution for user-provided paths, and
        // should never occur in compiler-generated ones.
        P::LeadingNameAccess_::GlobalAddress(name) => {
            context.env.add_diag(ice!((
                loc,
                "Found an address in top-level address position that uses a global name"
            )));
            Address::NamedUnassigned(name)
        }
        P::LeadingNameAccess_::Name(name) => {
            match named_address_mapping.get(&name.value).copied() {
                Some(addr) => make_address(context, name, loc, addr),
                None => {
                    if name_res.is_ok() {
                        context.env.add_diag(address_without_value_error(
                            suggest_declaration,
                            loc,
                            &name,
                        ));
                    }
                    Address::NamedUnassigned(name)
                }
            }
        }
    }
}

fn top_level_address_opt(context: &mut DefnContext, ln: P::LeadingNameAccess) -> Option<Address> {
    let name_res = check_valid_address_name(context, &ln);
    let named_address_mapping = context.named_address_mapping.as_ref().unwrap();
    let sp!(loc, ln_) = ln;
    match ln_ {
        P::LeadingNameAccess_::AnonymousAddress(bytes) => {
            debug_assert!(name_res.is_ok());
            Some(Address::anonymous(loc, bytes))
        }
        // This should have been handled elsewhere in alias resolution for user-provided paths, and
        // should never occur in compiler-generated ones.
        P::LeadingNameAccess_::GlobalAddress(_) => {
            context.env.add_diag(ice!((
                loc,
                "Found an address in top-level address position that uses a global name"
            )));
            None
        }
        P::LeadingNameAccess_::Name(name) => {
            let addr = named_address_mapping.get(&name.value).copied()?;
            Some(make_address(context, name, loc, addr))
        }
    }
}

fn maybe_make_well_known_address(context: &mut Context, loc: Loc, name: Symbol) -> Option<Address> {
    let named_address_mapping = context.defn_context.named_address_mapping.as_ref().unwrap();
    let addr = named_address_mapping.get(&name).copied()?;
    Some(make_address(
        &mut context.defn_context,
        sp(loc, name),
        loc,
        addr,
    ))
}

fn address_without_value_error(suggest_declaration: bool, loc: Loc, n: &Name) -> Diagnostic {
    let mut msg = format!("address '{}' is not assigned a value", n);
    if suggest_declaration {
        msg = format!(
            "{}. Try assigning it a value when calling the compiler",
            msg,
        )
    }
    diag!(NameResolution::AddressWithoutValue, (loc, msg))
}

fn make_address(
    context: &mut DefnContext,
    name: Name,
    loc: Loc,
    value: NumericalAddress,
) -> Address {
    Address::Numerical {
        name: Some(name),
        value: sp(loc, value),
        name_conflict: context.address_conflicts.contains(&name.value),
    }
}

fn module_ident(context: &mut DefnContext, sp!(loc, mident_): P::ModuleIdent) -> ModuleIdent {
    let P::ModuleIdent_ {
        address: ln,
        module,
    } = mident_;
    let addr = top_level_address(context, /* suggest_declaration */ false, ln);
    sp(loc, ModuleIdent_::new(addr, module))
}

fn check_module_address(
    context: &mut Context,
    loc: Loc,
    addr: Address,
    m: &mut P::ModuleDefinition,
) -> Spanned<Address> {
    let module_address = std::mem::take(&mut m.address);
    match module_address {
        Some(other_paddr) => {
            let other_loc = other_paddr.loc;
            let other_addr = top_level_address(
                &mut context.defn_context,
                /* suggest_declaration */ true,
                other_paddr,
            );
            let msg = if addr == other_addr {
                "Redundant address specification"
            } else {
                "Multiple addresses specified for module"
            };
            context.env().add_diag(diag!(
                Declarations::DuplicateItem,
                (other_loc, msg),
                (loc, "Address previously specified here")
            ));
            sp(other_loc, other_addr)
        }
        None => sp(loc, addr),
    }
}

fn duplicate_module(
    context: &mut Context,
    module_map: &UniqueMap<ModuleIdent, E::ModuleDefinition>,
    mident: ModuleIdent,
    old_loc: Loc,
) {
    let old_mident = module_map.get_key(&mident).unwrap();
    let dup_msg = format!("Duplicate definition for module '{}'", mident);
    let prev_msg = format!("Module previously defined here, with '{}'", old_mident);
    context.env().add_diag(diag!(
        Declarations::DuplicateItem,
        (mident.loc, dup_msg),
        (old_loc, prev_msg),
    ))
}

fn module(
    context: &mut Context,
    module_map: &mut UniqueMap<ModuleIdent, E::ModuleDefinition>,
    package_name: Option<Symbol>,
    module_address: Option<Spanned<Address>>,
    module_def: P::ModuleDefinition,
) {
    assert!(context.address.is_none());
    if module_def.is_spec_module {
        context.spec_deprecated(module_def.name.0.loc, /* is_error */ false);
        return;
    }
    let (mident, mod_) = module_(context, package_name, module_address, module_def);
    if let Err((mident, old_loc)) = module_map.add(mident, mod_) {
        duplicate_module(context, module_map, mident, old_loc)
    }
    context.address = None
}

fn set_module_address(
    context: &mut Context,
    module_name: &ModuleName,
    address: Option<Spanned<Address>>,
) {
    context.address = Some(match address {
        Some(sp!(_, addr)) => addr,
        None => {
            let loc = module_name.loc();
            let msg = format!(
                "Invalid module declaration. The module does not have a specified address. Either \
                 declare it inside of an 'address <address> {{' block or declare it with an \
                 address 'module <address>::{}''",
                module_name
            );
            context
                .env()
                .add_diag(diag!(Declarations::InvalidModule, (loc, msg)));
            Address::anonymous(loc, NumericalAddress::DEFAULT_ERROR_ADDRESS)
        }
    })
}

fn module_(
    context: &mut Context,
    package_name: Option<Symbol>,
    module_address: Option<Spanned<Address>>,
    mdef: P::ModuleDefinition,
) -> (ModuleIdent, E::ModuleDefinition) {
    let P::ModuleDefinition {
        attributes,
        loc,
        address,
        is_spec_module: _,
        name,
        members,
    } = mdef;
    let attributes = flatten_attributes(context, AttributePosition::Module, attributes);
    let mut warning_filter = module_warning_filter(context, &attributes);
    let config = context.env().package_config(package_name);
    warning_filter.union(&config.warning_filter);

    context
        .env()
        .add_warning_filter_scope(warning_filter.clone());
    assert!(context.address.is_none());
    assert!(address.is_none());
    set_module_address(context, &name, module_address);
    let _ = check_restricted_name_all_cases(&mut context.defn_context, NameCase::Module, &name.0);
    if name.value().starts_with(|c| c == '_') {
        let msg = format!(
            "Invalid module name '{}'. Module names cannot start with '_'",
            name,
        );
        context
            .env()
            .add_diag(diag!(Declarations::InvalidName, (name.loc(), msg)));
    }

    let name_loc = name.0.loc;
    let current_module = sp(name_loc, ModuleIdent_::new(*context.cur_address(), name));

    let mut new_scope = context.new_alias_map_builder();
    let mut use_funs_builder = UseFunsBuilder::new();
    module_self_aliases(&mut new_scope, &current_module);
    let members = members
        .into_iter()
        .filter_map(|member| {
            aliases_from_member(
                context,
                &mut new_scope,
                &mut use_funs_builder,
                &current_module,
                member,
            )
        })
        .collect::<Vec<_>>();
    context.push_alias_scope(loc, new_scope);

    let mut friends = UniqueMap::new();
    let mut functions = UniqueMap::new();
    let mut constants = UniqueMap::new();
    let mut structs = UniqueMap::new();
    for member in members {
        match member {
            P::ModuleMember::Use(_) => unreachable!(),
            P::ModuleMember::Friend(f) => friend(context, &mut friends, f),
            P::ModuleMember::Function(mut f) => {
                if !context.is_source_definition && f.macro_.is_none() {
                    f.body.value = P::FunctionBody_::Native
                }
                function(
                    context,
                    Some((current_module, &mut use_funs_builder)),
                    &mut functions,
                    f,
                )
            }
            P::ModuleMember::Constant(c) => constant(context, &mut constants, c),
            P::ModuleMember::Struct(s) => struct_def(context, &mut structs, s),
            P::ModuleMember::Spec(s) => context.spec_deprecated(s.loc, /* is_error */ false),
        }
    }
    let mut use_funs = use_funs(context, use_funs_builder);
    check_visibility_modifiers(context, &functions, &friends, package_name);

    context.pop_alias_scope(Some(&mut use_funs));

    let def = E::ModuleDefinition {
        package_name,
        attributes,
        loc,
        use_funs,
        is_source_module: context.is_source_definition,
        friends,
        structs,
        constants,
        functions,
        warning_filter,
    };
    context.env().pop_warning_filter_scope();
    (current_module, def)
}

fn check_visibility_modifiers(
    context: &mut Context,
    functions: &UniqueMap<FunctionName, E::Function>,
    friends: &UniqueMap<ModuleIdent, E::Friend>,
    package_name: Option<Symbol>,
) {
    let pub_package_enabled = context
        .env()
        .supports_feature(package_name, FeatureGate::PublicPackage);
    let edition = context.env().edition(package_name);
    // mark friend as deprecated
    if pub_package_enabled {
        let friend_msg = &format!(
            "'friend's are deprecated. Remove and replace '{}' with '{}'",
            E::Visibility::FRIEND,
            E::Visibility::PACKAGE,
        );
        let pub_msg = &format!(
            "'{}' is deprecated. Replace with '{}'",
            E::Visibility::FRIEND,
            E::Visibility::PACKAGE
        );
        for (_, _, friend_decl) in friends {
            let loc = friend_decl.loc;
            let diag = if edition == Edition::E2024_MIGRATION {
                for aloc in &friend_decl.attr_locs {
                    context
                        .env()
                        .add_diag(diag!(Migration::RemoveFriend, (*aloc, friend_msg)));
                }
                diag!(Migration::RemoveFriend, (loc, friend_msg))
            } else {
                diag!(Editions::DeprecatedFeature, (loc, friend_msg))
            };
            context.env().add_diag(diag);
        }
        for (_, _, function) in functions {
            let E::Visibility::Friend(loc) = function.visibility else {
                continue;
            };
            let diag = if edition == Edition::E2024_MIGRATION {
                diag!(Migration::MakePubPackage, (loc, pub_msg))
            } else {
                diag!(Editions::DeprecatedFeature, (loc, pub_msg))
            };
            context.env().add_diag(diag);
        }
    }

    // mark conflicting friend usage
    let mut friend_usage = friends.iter().next().map(|(_, _, friend)| friend.loc);
    let mut public_package_usage = None;
    for (_, _, function) in functions {
        match function.visibility {
            E::Visibility::Friend(loc) if friend_usage.is_none() => {
                friend_usage = Some(loc);
            }
            E::Visibility::Package(loc) => {
                context
                    .env()
                    .check_feature(package_name, FeatureGate::PublicPackage, loc);
                public_package_usage = Some(loc);
            }
            _ => (),
        }
    }

    // Emit any errors.
    if public_package_usage.is_some() && friend_usage.is_some() {
        let friend_error_msg = format!(
            "Cannot define 'friend' modules and use '{}' visibility in the same module",
            E::Visibility::PACKAGE
        );
        let package_definition_msg = format!("'{}' visibility used here", E::Visibility::PACKAGE);
        for (_, _, friend) in friends {
            context.env().add_diag(diag!(
                Declarations::InvalidVisibilityModifier,
                (friend.loc, friend_error_msg.clone()),
                (
                    public_package_usage.unwrap(),
                    package_definition_msg.clone()
                )
            ));
        }
        let package_error_msg = format!(
            "Cannot mix '{}' and '{}' visibilities in the same module",
            E::Visibility::PACKAGE_IDENT,
            E::Visibility::FRIEND_IDENT
        );
        let friend_error_msg = format!(
            "Cannot mix '{}' and '{}' visibilities in the same module",
            E::Visibility::FRIEND_IDENT,
            E::Visibility::PACKAGE_IDENT
        );
        for (_, _, function) in functions {
            match function.visibility {
                E::Visibility::Friend(loc) => {
                    context.env().add_diag(diag!(
                        Declarations::InvalidVisibilityModifier,
                        (loc, friend_error_msg.clone()),
                        (
                            public_package_usage.unwrap(),
                            package_definition_msg.clone()
                        )
                    ));
                }
                E::Visibility::Package(loc) => {
                    context.env().add_diag(diag!(
                        Declarations::InvalidVisibilityModifier,
                        (loc, package_error_msg.clone()),
                        (
                            friend_usage.unwrap(),
                            &format!("'{}' visibility used here", E::Visibility::FRIEND_IDENT)
                        )
                    ));
                }
                _ => {}
            }
        }
    }
}

fn flatten_attributes(
    context: &mut Context,
    attr_position: AttributePosition,
    attributes: Vec<P::Attributes>,
) -> E::Attributes {
    let all_attrs = attributes
        .into_iter()
        .flat_map(|attrs| attrs.value)
        .flat_map(|attr| attribute(context, attr_position, attr))
        .collect::<Vec<_>>();
    known_attributes(context, attr_position, all_attrs)
}

fn known_attributes(
    context: &mut Context,
    attr_position: AttributePosition,
    attributes: impl IntoIterator<Item = E::Attribute>,
) -> E::Attributes {
    let attributes = unique_attributes(context, attr_position, false, attributes);
    UniqueMap::maybe_from_iter(attributes.into_iter().filter_map(|(n, attr)| match n {
        sp!(loc, E::AttributeName_::Unknown(n)) => {
            let msg = format!(
                "Unknown attribute '{n}'. Custom attributes must be wrapped in '{ext}', \
                e.g. #[{ext}({n})]",
                ext = known_attributes::ExternalAttribute::EXTERNAL
            );
            context
                .env()
                .add_diag(diag!(Declarations::UnknownAttribute, (loc, msg)));
            None
        }
        sp!(loc, E::AttributeName_::Known(n)) => {
            gate_known_attribute(context, loc, &n);
            Some((sp(loc, n), attr))
        }
    }))
    .unwrap()
}

fn gate_known_attribute(context: &mut Context, loc: Loc, known: &KnownAttribute) {
    match known {
        KnownAttribute::Testing(_)
        | KnownAttribute::Verification(_)
        | KnownAttribute::Native(_)
        | KnownAttribute::Diagnostic(_)
        | KnownAttribute::DefinesPrimitive(_)
        | KnownAttribute::External(_)
        | KnownAttribute::Syntax(_) => (),
        KnownAttribute::Error(_) => {
            let pkg = context.current_package();
            context
                .env()
                .check_feature(pkg, FeatureGate::CleverAssertions, loc);
        }
    }
}

fn unique_attributes(
    context: &mut Context,
    attr_position: AttributePosition,
    is_nested: bool,
    attributes: impl IntoIterator<Item = E::Attribute>,
) -> E::InnerAttributes {
    let mut attr_map = UniqueMap::new();
    for sp!(loc, attr_) in attributes {
        let sp!(nloc, sym) = match &attr_ {
            E::Attribute_::Name(n)
            | E::Attribute_::Assigned(n, _)
            | E::Attribute_::Parameterized(n, _) => *n,
        };
        let name_ = match known_attributes::KnownAttribute::resolve(sym) {
            None => E::AttributeName_::Unknown(sym),
            Some(known) => {
                debug_assert!(known.name() == sym.as_str());
                if is_nested {
                    let msg = "Known attribute '{}' is not expected in a nested attribute position";
                    context
                        .env()
                        .add_diag(diag!(Declarations::InvalidAttribute, (nloc, msg)));
                    continue;
                }

                let expected_positions = known.expected_positions();
                if !expected_positions.contains(&attr_position) {
                    let msg = format!(
                        "Known attribute '{}' is not expected with a {}",
                        known.name(),
                        attr_position
                    );
                    let all_expected = expected_positions
                        .iter()
                        .map(|p| format!("{}", p))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let expected_msg = format!(
                        "Expected to be used with one of the following: {}",
                        all_expected
                    );
                    context.env().add_diag(diag!(
                        Declarations::InvalidAttribute,
                        (nloc, msg),
                        (nloc, expected_msg)
                    ));
                    continue;
                }
                E::AttributeName_::Known(known)
            }
        };
        if matches!(
            name_,
            E::AttributeName_::Known(KnownAttribute::Verification(_))
        ) {
            context.spec_deprecated(loc, /* is_error */ false)
        }
        if let Err((_, old_loc)) = attr_map.add(sp(nloc, name_), sp(loc, attr_)) {
            let msg = format!("Duplicate attribute '{}' attached to the same item", name_);
            context.env().add_diag(diag!(
                Declarations::DuplicateItem,
                (loc, msg),
                (old_loc, "Attribute previously given here"),
            ));
        }
    }
    attr_map
}

fn attribute(
    context: &mut Context,
    attr_position: AttributePosition,
    sp!(loc, attribute_): P::Attribute,
) -> Option<E::Attribute> {
    use E::Attribute_ as EA;
    use P::Attribute_ as PA;
    Some(sp(
        loc,
        match attribute_ {
            PA::Name(n) => EA::Name(n),
            PA::Assigned(n, v) => EA::Assigned(n, Box::new(context.attribute_value(*v)?)),
            PA::Parameterized(n, sp!(_, pattrs_)) => {
                let attrs = pattrs_
                    .into_iter()
                    .map(|a| attribute(context, attr_position, a))
                    .collect::<Option<Vec<_>>>()?;
                EA::Parameterized(n, unique_attributes(context, attr_position, true, attrs))
            }
        },
    ))
}

/// Like warning_filter, but it will filter _all_ warnings for non-source definitions (or for any
/// dependency packages)
fn module_warning_filter(context: &mut Context, attributes: &E::Attributes) -> WarningFilters {
    let filters = warning_filter(context, attributes);
    let is_dep = !context.is_source_definition || {
        let pkg = context.current_package();
        context.env().package_config(pkg).is_dependency
    };
    if is_dep {
        // For dependencies (non source defs or package deps), we check the filters for errors
        // but then throw them away and actually ignore _all_ warnings
        context.all_filter_alls.clone()
    } else {
        filters
    }
}

/// Finds the warning filters from the #[allow(_)] attribute and the deprecated #[lint_allow(_)]
/// attribute.
fn warning_filter(context: &mut Context, attributes: &E::Attributes) -> WarningFilters {
    let mut warning_filters = WarningFilters::new_for_source();
    let mut prefixed_filters: Vec<(DiagnosticAttribute, Option<Symbol>, Vec<Name>)> = vec![];
    // Gather lint_allow warnings
    if let Some(lint_allow_attr) = attributes.get_(&DiagnosticAttribute::LintAllow.into()) {
        // get the individual filters
        let inners =
            get_allow_attribute_inners(context, DiagnosticAttribute::LINT_ALLOW, lint_allow_attr);
        if let Some(inners) = inners {
            let names = prefixed_warning_filters(context, DiagnosticAttribute::LINT_ALLOW, inners);
            prefixed_filters.push((DiagnosticAttribute::LintAllow, Some(symbol!("lint")), names));
        }
    }
    // Gather allow warnings
    if let Some(allow_attr) = attributes.get_(&DiagnosticAttribute::Allow.into()) {
        // get the individual filters, or nested filters
        let inners = get_allow_attribute_inners(context, DiagnosticAttribute::ALLOW, allow_attr);
        for (inner_attr_loc, _, inner_attr) in inners.into_iter().flatten() {
            let (prefix, names) = match &inner_attr.value {
                // a filter, e.g. allow(unused_variables)
                E::Attribute_::Name(n) => (None, vec![*n]),
                // a nested filter, e.g. allow(lint(_))
                E::Attribute_::Parameterized(prefix, inners) => (
                    Some(prefix.value),
                    prefixed_warning_filters(context, prefix, inners),
                ),
                E::Attribute_::Assigned(n, _) => {
                    let msg = format!(
                        "Expected a stand alone warning filter identifier, e.g. '{}({})'",
                        DiagnosticAttribute::ALLOW,
                        n
                    );
                    context
                        .env()
                        .add_diag(diag!(Declarations::InvalidAttribute, (inner_attr_loc, msg)));
                    (None, vec![*n])
                }
            };
            prefixed_filters.push((DiagnosticAttribute::Allow, prefix, names));
        }
    }
    // Find the warning filter for each prefix+name instance
    for (diag_attr, prefix, names) in prefixed_filters {
        for sp!(nloc, n_) in names {
            let filters = context.env().filter_from_str(prefix, n_);
            if filters.is_empty() {
                let msg = match diag_attr {
                    DiagnosticAttribute::Allow => {
                        format!("Unknown warning filter '{}'", format_allow_attr(prefix, n_))
                    }
                    DiagnosticAttribute::LintAllow => {
                        // specialized error message for the deprecated syntax
                        format!(
                            "Unknown warning filter '{}({})'",
                            DiagnosticAttribute::LINT_ALLOW,
                            n_
                        )
                    }
                };
                context
                    .env()
                    .add_diag(diag!(Attributes::ValueWarning, (nloc, msg)));
                continue;
            };
            for f in filters {
                warning_filters.add(f);
            }
        }
    }
    warning_filters
}

fn get_allow_attribute_inners<'a>(
    context: &mut Context,
    name: &'static str,
    allow_attr: &'a E::Attribute,
) -> Option<&'a E::InnerAttributes> {
    use crate::diagnostics::codes::Category;
    match &allow_attr.value {
        E::Attribute_::Parameterized(_, inner) if !inner.is_empty() => Some(inner),
        _ => {
            let msg = format!(
                "Expected list of warnings, e.g. '{}({})'",
                name,
                WarningFilter::Category {
                    prefix: None,
                    category: Category::UnusedItem as u8,
                    name: Some(FILTER_UNUSED)
                }
                .to_str()
                .unwrap(),
            );
            context
                .env()
                .add_diag(diag!(Attributes::ValueWarning, (allow_attr.loc, msg)));
            None
        }
    }
}

fn prefixed_warning_filters(
    context: &mut Context,
    prefix: impl std::fmt::Display,
    inners: &E::InnerAttributes,
) -> Vec<Name> {
    inners
        .key_cloned_iter()
        .map(|(_, inner_attr)| match inner_attr {
            sp!(_, E::Attribute_::Name(n)) => *n,
            sp!(
                loc,
                E::Attribute_::Assigned(n, _) | E::Attribute_::Parameterized(n, _)
            ) => {
                let msg = format!(
                    "Expected a warning filter identifier, e.g. '{}({}({}))'",
                    DiagnosticAttribute::ALLOW,
                    prefix,
                    n
                );
                context
                    .env()
                    .add_diag(diag!(Attributes::ValueWarning, (*loc, msg)));
                *n
            }
        })
        .collect()
}

//**************************************************************************************************
// Name Access Chain (Path) Resolution
//**************************************************************************************************

#[derive(Clone, Copy)]
enum Access {
    Type,
    ApplyNamed,
    ApplyPositional,
    Term,
    Module, // Just used for errors
}

// This trait describes the commands available to handle alias scopes and expanding name access
// chains. This is used to model both legacy and modern path expansion.

trait PathExpander {
    // Push a new innermost alias scope
    fn push_alias_scope(
        &mut self,
        loc: Loc,
        new_scope: AliasMapBuilder,
    ) -> Result<Vec<UnnecessaryAlias>, Box<Diagnostic>>;

    // Push a number of type parameters onto the alias information in the path expander. They are
    // never resolved, but are tracked to apply appropriate shadowing.
    fn push_type_parameters(&mut self, tparams: Vec<&Name>);

    // Pop the innermost alias scope
    fn pop_alias_scope(&mut self) -> AliasSet;

    fn name_access_chain_to_attribute_value(
        &mut self,
        context: &mut DefnContext,
        attribute_value: P::AttributeValue,
    ) -> Option<E::AttributeValue>;

    fn name_access_chain_to_module_access(
        &mut self,
        context: &mut DefnContext,
        access: Access,
        name_chain: P::NameAccessChain,
    ) -> Option<E::ModuleAccess>;

    fn name_access_chain_to_module_ident(
        &mut self,
        context: &mut DefnContext,
        name_chain: P::NameAccessChain,
    ) -> Option<E::ModuleIdent>;
}

// -----------------------------------------------
// Legacy Implementation

struct LegacyPathExpander {
    aliases: legacy_aliases::AliasMap,
    old_alias_maps: Vec<legacy_aliases::OldAliasMap>,
}

impl LegacyPathExpander {
    fn new() -> LegacyPathExpander {
        LegacyPathExpander {
            aliases: legacy_aliases::AliasMap::new(),
            old_alias_maps: vec![],
        }
    }
}

impl PathExpander for LegacyPathExpander {
    fn push_alias_scope(
        &mut self,
        loc: Loc,
        new_scope: AliasMapBuilder,
    ) -> Result<Vec<UnnecessaryAlias>, Box<Diagnostic>> {
        self.old_alias_maps
            .push(self.aliases.add_and_shadow_all(loc, new_scope)?);
        Ok(vec![])
    }

    fn push_type_parameters(&mut self, tparams: Vec<&Name>) {
        self.old_alias_maps
            .push(self.aliases.shadow_for_type_parameters(tparams));
    }

    fn pop_alias_scope(&mut self) -> AliasSet {
        if let Some(outer_scope) = self.old_alias_maps.pop() {
            self.aliases.set_to_outer_scope(outer_scope)
        } else {
            AliasSet::new()
        }
    }

    fn name_access_chain_to_attribute_value(
        &mut self,
        context: &mut DefnContext,
        sp!(loc, avalue_): P::AttributeValue,
    ) -> Option<E::AttributeValue> {
        use E::AttributeValue_ as EV;
        use P::{AttributeValue_ as PV, LeadingNameAccess_ as LN, NameAccessChain_ as PN};
        Some(sp(
            loc,
            match avalue_ {
                PV::Value(v) => EV::Value(value(context, v)?),
                PV::ModuleAccess(
                    sp!(ident_loc, PN::Two(sp!(aloc, LN::AnonymousAddress(a)), n)),
                ) => {
                    let addr = Address::anonymous(aloc, a);
                    let mident = sp(ident_loc, ModuleIdent_::new(addr, ModuleName(n)));
                    if context.module_members.get(&mident).is_none() {
                        context.env.add_diag(diag!(
                            NameResolution::UnboundModule,
                            (ident_loc, format!("Unbound module '{}'", mident))
                        ));
                    }
                    EV::Module(mident)
                }
                // bit wonky, but this is the only spot currently where modules and expressions exist
                // in the same namespace.
                // TODO consider if we want to just force all of these checks into the well-known
                // attribute setup
                PV::ModuleAccess(sp!(ident_loc, PN::One(n)))
                    if self.aliases.module_alias_get(&n).is_some() =>
                {
                    let sp!(_, mident_) = self.aliases.module_alias_get(&n).unwrap();
                    let mident = sp(ident_loc, mident_);
                    if context.module_members.get(&mident).is_none() {
                        context.env.add_diag(diag!(
                            NameResolution::UnboundModule,
                            (ident_loc, format!("Unbound module '{}'", mident))
                        ));
                    }
                    EV::Module(mident)
                }
                PV::ModuleAccess(sp!(ident_loc, PN::Two(sp!(aloc, LN::Name(n1)), n2)))
                    if context
                        .named_address_mapping
                        .as_ref()
                        .map(|m| m.contains_key(&n1.value))
                        .unwrap_or(false) =>
                {
                    let addr = top_level_address(
                        context,
                        /* suggest_declaration */ false,
                        sp(aloc, LN::Name(n1)),
                    );
                    let mident = sp(ident_loc, ModuleIdent_::new(addr, ModuleName(n2)));
                    if context.module_members.get(&mident).is_none() {
                        context.env.add_diag(diag!(
                            NameResolution::UnboundModule,
                            (ident_loc, format!("Unbound module '{}'", mident))
                        ));
                    }
                    EV::Module(mident)
                }
                PV::ModuleAccess(ma) => EV::ModuleAccess(self.name_access_chain_to_module_access(
                    context,
                    Access::Type,
                    ma,
                )?),
            },
        ))
    }

    fn name_access_chain_to_module_access(
        &mut self,
        context: &mut DefnContext,
        access: Access,
        sp!(loc, ptn_): P::NameAccessChain,
    ) -> Option<E::ModuleAccess> {
        use E::ModuleAccess_ as EN;
        use P::{LeadingNameAccess_ as LN, NameAccessChain_ as PN};

        let tn_ = match (access, ptn_) {
            (Access::ApplyPositional, PN::One(n))
            | (Access::ApplyNamed, PN::One(n))
            | (Access::Type, PN::One(n)) => match self.aliases.member_alias_get(&n) {
                Some((mident, mem)) => EN::ModuleAccess(mident, mem),
                None => EN::Name(n),
            },
            (Access::Term, PN::One(n)) if is_valid_struct_or_constant_name(n.value.as_str()) => {
                match self.aliases.member_alias_get(&n) {
                    Some((mident, mem)) => EN::ModuleAccess(mident, mem),
                    None => EN::Name(n),
                }
            }
            (Access::Term, PN::One(n)) => EN::Name(n),
            (Access::Module, PN::One(_n)) => {
                context.env.add_diag(ice!((
                    loc,
                    "ICE path resolution produced an impossible path for a module"
                )));
                return None;
            }
            (_, PN::Two(sp!(nloc, LN::AnonymousAddress(_)), _)) => {
                let diag = unexpected_address_module_error(loc, nloc, access);
                context.env.add_diag(diag);
                return None;
            }

            (_, PN::Two(sp!(_, LN::Name(n1)), n2)) => match self.aliases.module_alias_get(&n1) {
                None => {
                    context.env.add_diag(diag!(
                        NameResolution::UnboundModule,
                        (n1.loc, format!("Unbound module alias '{}'", n1))
                    ));
                    return None;
                }
                Some(mident) => EN::ModuleAccess(mident, n2),
            },
            (_, PN::Two(sp!(eloc, LN::GlobalAddress(_)), _)) => {
                let mut diag: Diagnostic = create_feature_error(
                    context.env.edition(None), // We already know we are failing, so no package.
                    FeatureGate::Move2024Paths,
                    eloc,
                );
                diag.add_secondary_label((
                    eloc,
                    "Paths that start with `::` are not valid in legacy move.",
                ));
                context.env.add_diag(diag);
                return None;
            }
            (_, PN::Three(sp!(ident_loc, (ln, n2)), n3)) => {
                let addr = top_level_address(context, /* suggest_declaration */ false, ln);
                let mident = sp(ident_loc, ModuleIdent_::new(addr, ModuleName(n2)));
                EN::ModuleAccess(mident, n3)
            }
        };
        Some(sp(loc, tn_))
    }

    fn name_access_chain_to_module_ident(
        &mut self,
        context: &mut DefnContext,
        sp!(loc, pn_): P::NameAccessChain,
    ) -> Option<E::ModuleIdent> {
        use P::NameAccessChain_ as PN;
        match pn_ {
            PN::One(name) => match self.aliases.module_alias_get(&name) {
                None => {
                    context.env.add_diag(diag!(
                        NameResolution::UnboundModule,
                        (name.loc, format!("Unbound module alias '{}'", name)),
                    ));
                    None
                }
                Some(mident) => Some(mident),
            },
            PN::Two(ln, n) => {
                let pmident_ = P::ModuleIdent_ {
                    address: ln,
                    module: ModuleName(n),
                };
                Some(module_ident(context, sp(loc, pmident_)))
            }
            PN::Three(sp!(ident_loc, (ln, n)), mem) => {
                // Process the module ident just for errors
                let pmident_ = P::ModuleIdent_ {
                    address: ln,
                    module: ModuleName(n),
                };
                let _ = module_ident(context, sp(ident_loc, pmident_));
                context.env.add_diag(diag!(
                    NameResolution::NamePositionMismatch,
                    (
                        mem.loc,
                        "Unexpected module member access. Expected a module identifier only",
                    )
                ));
                None
            }
        }
    }
}

fn unexpected_address_module_error(loc: Loc, nloc: Loc, access: Access) -> Diagnostic {
    let case = match access {
        Access::Type | Access::ApplyNamed | Access::ApplyPositional => "type",
        Access::Term => "expression",
        Access::Module => {
            return ice!(
                (
                    loc,
                    "ICE expected a module name and got one, but tried to report an error"
                ),
                (nloc, "Name location")
            )
        }
    };
    let unexpected_msg = format!(
        "Unexpected module identifier. A module identifier is not a valid {}",
        case
    );
    diag!(
        NameResolution::NamePositionMismatch,
        (loc, unexpected_msg),
        (nloc, "Expected a module name".to_owned()),
    )
}

// -----------------------------------------------
// Move 2024 Implementation

struct Move2024PathExpander {
    aliases: AliasMap,
}

#[derive(Debug, PartialEq, Eq)]
enum AccessChainResult {
    ModuleAccess(Loc, E::ModuleAccess_),
    Address(Loc, E::Address),
    ModuleIdent(Loc, E::ModuleIdent),
    UnresolvedName(Loc, Name),
    ResolutionFailure(Box<AccessChainResult>, AccessChainFailure),
}

#[derive(Debug, PartialEq, Eq)]
enum AccessChainFailure {
    UnresolvedAlias(Name),
    InvalidKind(String),
}

impl Move2024PathExpander {
    fn new() -> Move2024PathExpander {
        Move2024PathExpander {
            aliases: AliasMap::new(),
        }
    }

    fn resolve_root(
        &mut self,
        context: &mut DefnContext,
        sp!(loc, name): P::LeadingNameAccess,
    ) -> AccessChainResult {
        use AccessChainFailure::*;
        use AccessChainResult::*;
        use P::LeadingNameAccess_ as LN;
        match name {
            LN::AnonymousAddress(address) => Address(loc, E::Address::anonymous(loc, address)),
            LN::GlobalAddress(name) => {
                if let Some(address) = context
                    .named_address_mapping
                    .expect("ICE no named address mapping")
                    .get(&name.value)
                {
                    Address(loc, make_address(context, name, name.loc, *address))
                } else {
                    ResolutionFailure(Box::new(UnresolvedName(loc, name)), UnresolvedAlias(name))
                }
            }
            LN::Name(name) => match self.resolve_name(context, NameSpace::LeadingAccess, name) {
                result @ UnresolvedName(_, _) => {
                    ResolutionFailure(Box::new(result), UnresolvedAlias(name))
                }
                other => other,
            },
        }
    }

    fn resolve_name(
        &mut self,
        context: &mut DefnContext,
        namespace: NameSpace,
        name: Name,
    ) -> AccessChainResult {
        use AccessChainFailure::*;
        use AccessChainResult::*;
        use E::ModuleAccess_ as EN;

        match self.aliases.resolve(namespace, &name) {
            Some(AliasEntry::Member(_, mident, sp!(_, mem))) => {
                // We are preserving the name's original location, rather than referring to where
                // the alias was defined. The name represents JUST the member name, though, so we do
                // not change location of the module as we don't have this information.
                // TODO maybe we should also keep the alias reference (or its location)?
                ModuleAccess(name.loc, EN::ModuleAccess(mident, sp(name.loc, mem)))
            }
            Some(AliasEntry::Module(_, mident)) => {
                // We are preserving the name's original location, rather than referring to where
                // the alias was defined. The name represents JUST the module name, though, so we do
                // not change location of the address as we don't have this information.
                // TODO maybe we should also keep the alias reference (or its location)?
                let sp!(
                    _,
                    ModuleIdent_ {
                        address,
                        module: ModuleName(sp!(_, module))
                    }
                ) = mident;
                let module = ModuleName(sp(name.loc, module));
                ModuleIdent(name.loc, sp(name.loc, ModuleIdent_ { address, module }))
            }
            Some(AliasEntry::Address(_, address)) => {
                Address(name.loc, make_address(context, name, name.loc, address))
            }
            Some(AliasEntry::TypeParam(_)) => {
                context.env.add_diag(ice!((
                    name.loc,
                    "ICE alias map misresolved name as type param"
                )));
                UnresolvedName(name.loc, name)
            }
            None => {
                if let Some(entry) = self.aliases.resolve_any_for_error(&name) {
                    let msg = match namespace {
                        NameSpace::ModuleMembers => "a type, function, or constant".to_string(),
                        // we exclude types from this message since it would have been caught in
                        // the other namespace
                        NameSpace::LeadingAccess => "an address or module".to_string(),
                    };
                    let result = match entry {
                        AliasEntry::Address(_, address) => {
                            Address(name.loc, make_address(context, name, name.loc, address))
                        }
                        AliasEntry::Module(_, mident) => ModuleIdent(name.loc, mident),
                        AliasEntry::Member(_, mident, mem) => {
                            ModuleAccess(name.loc, EN::ModuleAccess(mident, mem))
                        }
                        AliasEntry::TypeParam(_) => {
                            context.env.add_diag(ice!((
                                name.loc,
                                "ICE alias map misresolved name as type param"
                            )));
                            UnresolvedName(name.loc, name)
                        }
                    };
                    ResolutionFailure(Box::new(result), InvalidKind(msg))
                } else {
                    UnresolvedName(name.loc, name)
                }
            }
        }
    }

    fn resolve_name_access_chain(
        &mut self,
        context: &mut DefnContext,
        access: Access,
        sp!(loc, chain): P::NameAccessChain,
    ) -> AccessChainResult {
        use AccessChainFailure::*;
        use AccessChainResult::*;
        use E::ModuleAccess_ as EN;
        use P::NameAccessChain_ as PN;

        match chain {
            PN::One(name) => {
                use crate::naming::ast::BuiltinFunction_;
                use crate::naming::ast::BuiltinTypeName_;
                let namespace = match access {
                    Access::Type | Access::ApplyNamed | Access::ApplyPositional | Access::Term => {
                        NameSpace::ModuleMembers
                    }
                    Access::Module => NameSpace::LeadingAccess,
                };

                // This is a hack to let `use std::vector` play nicely with `vector`,
                // plus preserve things like `u64`, etc.
                if !matches!(access, Access::Module)
                    && (BuiltinFunction_::all_names().contains(&name.value)
                        || BuiltinTypeName_::all_names().contains(&name.value))
                {
                    AccessChainResult::UnresolvedName(name.loc, name)
                } else {
                    self.resolve_name(context, namespace, name)
                }
            }
            PN::Two(root_name, name) => match self.resolve_root(context, root_name) {
                Address(_, address) => {
                    ModuleIdent(loc, sp(loc, ModuleIdent_::new(address, ModuleName(name))))
                }
                ModuleIdent(_, mident) => ModuleAccess(loc, EN::ModuleAccess(mident, name)),
                result @ ModuleAccess(_, _) => ResolutionFailure(
                    Box::new(result),
                    InvalidKind("a module or address".to_string()),
                ),
                result @ ResolutionFailure(_, _) => result,
                result @ UnresolvedName(_, _) => {
                    context
                        .env
                        .add_diag(ice!((loc, "ICE access chain expansion failed")));
                    result
                }
            },
            PN::Three(sp!(ident_loc, (root_name, next_name)), last_name) => {
                match self.resolve_root(context, root_name) {
                    Address(_, address) => {
                        let mident =
                            sp(ident_loc, ModuleIdent_::new(address, ModuleName(next_name)));
                        ModuleAccess(loc, EN::ModuleAccess(mident, last_name))
                    }
                    // In Move Legacy, we always treated three-place names as fully-qualified. For
                    // migration mode, if we could have gotten the correct result doing so, we emit
                    // a migration change to globally-qualify that path and remediate the error.
                    result @ ModuleIdent(_, _)
                        if context.env.edition(context.current_package)
                            == Edition::E2024_MIGRATION =>
                    {
                        if let Some(address) = top_level_address_opt(context, root_name) {
                            context.env.add_diag(diag!(
                                Migration::NeedsGlobalQualification,
                                (root_name.loc, "Must globally qualify name")
                            ));
                            let mident =
                                sp(ident_loc, ModuleIdent_::new(address, ModuleName(next_name)));
                            ModuleAccess(loc, EN::ModuleAccess(mident, last_name))
                        } else {
                            ResolutionFailure(
                                Box::new(result),
                                InvalidKind("an address".to_string()),
                            )
                        }
                    }
                    result @ (ModuleIdent(_, _) | ModuleAccess(_, _)) => {
                        ResolutionFailure(Box::new(result), InvalidKind("an address".to_string()))
                    }
                    result @ ResolutionFailure(_, _) => result,
                    result @ UnresolvedName(_, _) => {
                        context
                            .env
                            .add_diag(ice!((loc, "ICE access chain expansion failed")));
                        result
                    }
                }
            }
        }
    }
}

impl PathExpander for Move2024PathExpander {
    fn push_alias_scope(
        &mut self,
        loc: Loc,
        new_scope: AliasMapBuilder,
    ) -> Result<Vec<UnnecessaryAlias>, Box<Diagnostic>> {
        self.aliases.push_alias_scope(loc, new_scope)
    }

    fn push_type_parameters(&mut self, tparams: Vec<&Name>) {
        self.aliases.push_type_parameters(tparams)
    }

    fn pop_alias_scope(&mut self) -> AliasSet {
        self.aliases.pop_scope()
    }

    fn name_access_chain_to_attribute_value(
        &mut self,
        context: &mut DefnContext,
        sp!(loc, avalue_): P::AttributeValue,
    ) -> Option<E::AttributeValue> {
        use E::AttributeValue_ as EV;
        use P::AttributeValue_ as PV;
        Some(sp(
            loc,
            match avalue_ {
                PV::Value(v) => EV::Value(value(context, v)?),
                // A bit strange, but we try to resolve it as a term and a module, and report
                // an error if they both resolve (to different things)
                PV::ModuleAccess(access_chain) => {
                    let term_result =
                        self.resolve_name_access_chain(context, Access::Term, access_chain.clone());
                    let module_result =
                        self.resolve_name_access_chain(context, Access::Module, access_chain);
                    let result = match (term_result, module_result) {
                        (t_res, m_res) if t_res == m_res => t_res,
                        (
                            AccessChainResult::ResolutionFailure(_, _)
                            | AccessChainResult::UnresolvedName(_, _),
                            other,
                        )
                        | (
                            other,
                            AccessChainResult::ResolutionFailure(_, _)
                            | AccessChainResult::UnresolvedName(_, _),
                        ) => other,
                        (t_res, m_res) => {
                            let msg = format!(
                                "Ambiguous attribute value. It can resolve to both {} and {}",
                                t_res.err_name(),
                                m_res.err_name()
                            );
                            context
                                .env
                                .add_diag(diag!(Attributes::AmbiguousAttributeValue, (loc, msg)));
                            return None;
                        }
                    };
                    match result {
                        AccessChainResult::ModuleIdent(_, mident) => {
                            if context.module_members.get(&mident).is_none() {
                                context.env.add_diag(diag!(
                                    NameResolution::UnboundModule,
                                    (loc, format!("Unbound module '{}'", mident))
                                ));
                            }
                            EV::Module(mident)
                        }
                        AccessChainResult::ModuleAccess(loc, access) => {
                            EV::ModuleAccess(sp(loc, access))
                        }
                        AccessChainResult::UnresolvedName(loc, name) => {
                            EV::ModuleAccess(sp(loc, E::ModuleAccess_::Name(name)))
                        }
                        AccessChainResult::Address(_, a) => EV::Address(a),
                        result @ AccessChainResult::ResolutionFailure(_, _) => {
                            context.env.add_diag(access_chain_resolution_error(result));
                            return None;
                        }
                    }
                }
            },
        ))
    }

    fn name_access_chain_to_module_access(
        &mut self,
        context: &mut DefnContext,
        access: Access,
        chain: P::NameAccessChain,
    ) -> Option<E::ModuleAccess> {
        use AccessChainResult::*;
        use E::ModuleAccess_ as EN;
        use P::NameAccessChain_ as PN;

        let loc = chain.loc;

        let module_access = match access {
            Access::ApplyPositional | Access::ApplyNamed | Access::Type => {
                let resolved_name = self.resolve_name_access_chain(context, access, chain.clone());
                match resolved_name {
                    UnresolvedName(_, name) => EN::Name(name),
                    ModuleAccess(_, access) => access,
                    Address(_, _) => {
                        context.env.add_diag(unexpected_access_error(
                            resolved_name.loc(),
                            "address".to_string(),
                            access,
                        ));
                        return None;
                    }
                    ModuleIdent(_, sp!(_, ModuleIdent_ { address, module })) => {
                        let mut diag = unexpected_access_error(
                            resolved_name.loc(),
                            "module".to_string(),
                            access,
                        );
                        let base_str = format!("{}", chain);
                        let realized_str = format!("{}::{}", address, module);
                        if base_str != realized_str {
                            diag.add_note(format!(
                                "Resolved '{}' to module identifier '{}'",
                                base_str, realized_str
                            ));
                        }
                        context.env.add_diag(diag);
                        return None;
                    }
                    result @ ResolutionFailure(_, _) => {
                        context.env.add_diag(access_chain_resolution_error(result));
                        return None;
                    }
                }
            }
            Access::Term => match chain.value {
                PN::One(name) if !is_valid_struct_or_constant_name(&name.to_string()) => {
                    EN::Name(name)
                }
                _ => {
                    let resolved_name = self.resolve_name_access_chain(context, access, chain);
                    match resolved_name {
                        UnresolvedName(_, name) => EN::Name(name),
                        ModuleAccess(_, access) => access,
                        Address(_, _) => {
                            context.env.add_diag(unexpected_access_error(
                                resolved_name.loc(),
                                "address".to_string(),
                                access,
                            ));
                            return None;
                        }
                        ModuleIdent(_, _) => {
                            context.env.add_diag(unexpected_access_error(
                                resolved_name.loc(),
                                "module".to_string(),
                                access,
                            ));
                            return None;
                        }
                        result @ ResolutionFailure(_, _) => {
                            context.env.add_diag(access_chain_resolution_error(result));
                            return None;
                        }
                    }
                }
            },
            Access::Module => {
                context.env.add_diag(ice!((
                    loc,
                    "ICE module access should never resolve to a module member"
                )));
                return None;
            }
        };
        Some(sp(loc, module_access))
    }

    fn name_access_chain_to_module_ident(
        &mut self,
        context: &mut DefnContext,
        chain: P::NameAccessChain,
    ) -> Option<E::ModuleIdent> {
        use AccessChainResult::*;
        let resolved_name = self.resolve_name_access_chain(context, Access::Module, chain);
        match resolved_name {
            ModuleIdent(_, mident) => Some(mident),
            UnresolvedName(_, name) => {
                context.env.add_diag(unbound_module_error(name));
                None
            }
            Address(_, _) => {
                context.env.add_diag(unexpected_access_error(
                    resolved_name.loc(),
                    "address".to_string(),
                    Access::Module,
                ));
                None
            }
            ModuleAccess(_, _) => {
                context.env.add_diag(unexpected_access_error(
                    resolved_name.loc(),
                    "module member".to_string(),
                    Access::Module,
                ));
                None
            }
            result @ ResolutionFailure(_, _) => {
                context.env.add_diag(access_chain_resolution_error(result));
                None
            }
        }
    }
}

impl AccessChainResult {
    fn loc(&self) -> Loc {
        match self {
            AccessChainResult::ModuleAccess(loc, _) => *loc,
            AccessChainResult::Address(loc, _) => *loc,
            AccessChainResult::ModuleIdent(loc, _) => *loc,
            AccessChainResult::UnresolvedName(loc, _) => *loc,
            AccessChainResult::ResolutionFailure(inner, _) => inner.loc(),
        }
    }

    fn err_name(&self) -> String {
        match self {
            AccessChainResult::ModuleAccess(_, _) => "a module member".to_string(),
            AccessChainResult::ModuleIdent(_, _) => "a module".to_string(),
            AccessChainResult::UnresolvedName(_, _) => "a name".to_string(),
            AccessChainResult::Address(_, _) => "an address".to_string(),
            AccessChainResult::ResolutionFailure(inner, _) => inner.err_name(),
        }
    }
}

fn unexpected_access_error(loc: Loc, result: String, access: Access) -> Diagnostic {
    let case = match access {
        Access::Type | Access::ApplyNamed => "type",
        Access::ApplyPositional => "expression",
        Access::Term => "expression",
        Access::Module => "module",
    };
    let unexpected_msg = if result.starts_with('a') {
        format!(
            "Unexpected {0} identifier. An {0} identifier is not a valid {1}",
            result, case
        )
    } else {
        format!(
            "Unexpected {0} identifier. A {0} identifier is not a valid {1}",
            result, case
        )
    };
    diag!(NameResolution::NamePositionMismatch, (loc, unexpected_msg),)
}

fn unbound_module_error(name: Name) -> Diagnostic {
    diag!(
        NameResolution::UnboundModule,
        (name.loc, format!("Unbound module alias '{}'", name))
    )
}

fn access_chain_resolution_error(result: AccessChainResult) -> Diagnostic {
    if let AccessChainResult::ResolutionFailure(inner, reason) = result {
        let loc = inner.loc();
        let msg = match reason {
            AccessChainFailure::InvalidKind(kind) => format!(
                "Expected {} in this position, not {}",
                kind,
                inner.err_name()
            ),
            AccessChainFailure::UnresolvedAlias(name) => {
                format!("Could not resolve the name '{}'", name)
            }
        };
        diag!(NameResolution::NamePositionMismatch, (loc, msg))
    } else {
        ice!((
            result.loc(),
            "ICE compiler miscalled access chain resolution error handler"
        ))
    }
}

//**************************************************************************************************
// Aliases
//**************************************************************************************************

fn all_module_members<'a>(
    context: &mut DefnContext,
    named_addr_maps: &NamedAddressMaps,
    members: &mut UniqueMap<ModuleIdent, ModuleMembers>,
    always_add: bool,
    defs: impl IntoIterator<Item = &'a P::PackageDefinition>,
) {
    for P::PackageDefinition {
        named_address_map: named_address_map_index,
        def,
        ..
    } in defs
    {
        let named_addr_map: &NamedAddressMap = named_addr_maps.get(*named_address_map_index);
        match def {
            P::Definition::Module(m) => {
                let addr = match &m.address {
                    Some(a) => top_level_address_(
                        context,
                        named_addr_map,
                        /* suggest_declaration */ true,
                        *a,
                    ),
                    // Error will be handled when the module is compiled
                    None => Address::anonymous(m.loc, NumericalAddress::DEFAULT_ERROR_ADDRESS),
                };
                module_members(members, always_add, addr, m)
            }
            P::Definition::Address(addr_def) => {
                let addr = top_level_address_(
                    context,
                    named_addr_map,
                    /* suggest_declaration */ false,
                    addr_def.addr,
                );
                for m in &addr_def.modules {
                    module_members(members, always_add, addr, m)
                }
            }
        };
    }
}

fn module_members(
    members: &mut UniqueMap<ModuleIdent, ModuleMembers>,
    always_add: bool,
    address: Address,
    m: &P::ModuleDefinition,
) {
    let mident = sp(m.name.loc(), ModuleIdent_::new(address, m.name));
    if !always_add && members.contains_key(&mident) {
        return;
    }
    let mut cur_members = members.remove(&mident).unwrap_or_default();
    for mem in &m.members {
        match mem {
            P::ModuleMember::Function(f) => {
                cur_members.insert(f.name.0, ModuleMemberKind::Function);
            }
            P::ModuleMember::Constant(c) => {
                cur_members.insert(c.name.0, ModuleMemberKind::Constant);
            }
            P::ModuleMember::Struct(s) => {
                cur_members.insert(s.name.0, ModuleMemberKind::Struct);
            }
            P::ModuleMember::Spec(_) => (),
            P::ModuleMember::Use(_) | P::ModuleMember::Friend(_) => (),
        };
    }
    members.add(mident, cur_members).unwrap();
}

fn named_addr_map_to_alias_map_builder(
    context: &mut Context,
    named_addr_map: &NamedAddressMap,
) -> AliasMapBuilder {
    let mut new_aliases = context.new_alias_map_builder();
    for (name, addr) in named_addr_map {
        // Address symbols get dummy locations so that we can lift them to names. These should
        // always be rewritten with more-accurate information as they are used.
        new_aliases
            .add_address_alias(sp(Loc::invalid(), *name), *addr)
            .expect("ICE dupe address");
    }
    new_aliases
}

fn module_self_aliases(acc: &mut AliasMapBuilder, current_module: &ModuleIdent) {
    let self_name = sp(current_module.loc, ModuleName::SELF_NAME.into());
    acc.add_implicit_module_alias(self_name, *current_module)
        .unwrap()
}

fn aliases_from_member(
    context: &mut Context,
    acc: &mut AliasMapBuilder,
    use_funs: &mut UseFunsBuilder,
    current_module: &ModuleIdent,
    member: P::ModuleMember,
) -> Option<P::ModuleMember> {
    macro_rules! check_name_and_add_implicit_alias {
        ($kind:expr, $name:expr) => {{
            if let Some(n) = check_valid_module_member_name(context, $kind, $name) {
                if let Err(loc) = acc.add_implicit_member_alias(
                    n.clone(),
                    current_module.clone(),
                    n.clone(),
                    $kind,
                ) {
                    duplicate_module_member(context, loc, n)
                }
            }
        }};
    }

    match member {
        P::ModuleMember::Use(u) => {
            use_(context, acc, use_funs, u);
            None
        }
        f @ P::ModuleMember::Friend(_) => {
            // friend declarations do not produce implicit aliases
            Some(f)
        }
        P::ModuleMember::Function(f) => {
            let n = f.name.0;
            check_name_and_add_implicit_alias!(ModuleMemberKind::Function, n);
            Some(P::ModuleMember::Function(f))
        }
        P::ModuleMember::Constant(c) => {
            let n = c.name.0;
            check_name_and_add_implicit_alias!(ModuleMemberKind::Constant, n);
            Some(P::ModuleMember::Constant(c))
        }
        P::ModuleMember::Struct(s) => {
            let n = s.name.0;
            check_name_and_add_implicit_alias!(ModuleMemberKind::Struct, n);
            Some(P::ModuleMember::Struct(s))
        }
        P::ModuleMember::Spec(s) => Some(P::ModuleMember::Spec(s)),
    }
}

fn uses(context: &mut Context, uses: Vec<P::UseDecl>) -> (AliasMapBuilder, UseFunsBuilder) {
    let mut new_scope = context.new_alias_map_builder();
    let mut use_funs = UseFunsBuilder::new();
    for u in uses {
        use_(context, &mut new_scope, &mut use_funs, u);
    }
    (new_scope, use_funs)
}

fn use_(
    context: &mut Context,
    acc: &mut AliasMapBuilder,
    use_funs: &mut UseFunsBuilder,
    u: P::UseDecl,
) {
    let P::UseDecl {
        use_: u,
        loc,
        attributes,
    } = u;
    let attributes = flatten_attributes(context, AttributePosition::Use, attributes);
    match u {
        P::Use::NestedModuleUses(address, use_decls) => {
            for (module, use_) in use_decls {
                let mident = sp(module.loc(), P::ModuleIdent_ { address, module });
                module_use(context, acc, use_funs, mident, &attributes, use_);
            }
        }
        P::Use::ModuleUse(mident, use_) => {
            module_use(context, acc, use_funs, mident, &attributes, use_);
        }
        P::Use::Fun {
            visibility,
            function,
            ty,
            method,
        } => {
            let pkg = context.current_package();
            context.env().check_feature(pkg, FeatureGate::DotCall, loc);
            let is_public = match visibility {
                P::Visibility::Public(vis_loc) => Some(vis_loc),
                P::Visibility::Internal => None,
                P::Visibility::Friend(vis_loc) | P::Visibility::Package(vis_loc) => {
                    let msg = "Invalid visibility for 'use fun' declaration";
                    let vis_msg = format!(
                        "Module level 'use fun' declarations can be '{}' for the module's types, \
                    otherwise they must internal to declared scope.",
                        P::Visibility::PUBLIC
                    );
                    context.env().add_diag(diag!(
                        Declarations::InvalidUseFun,
                        (loc, msg),
                        (vis_loc, vis_msg)
                    ));
                    None
                }
            };
            let explicit = ParserExplicitUseFun {
                loc,
                attributes,
                is_public,
                function,
                ty,
                method,
            };
            use_funs.explicit.push(explicit);
        }
    }
}

fn module_use(
    context: &mut Context,
    acc: &mut AliasMapBuilder,
    use_funs: &mut UseFunsBuilder,
    in_mident: P::ModuleIdent,
    attributes: &E::Attributes,
    muse: P::ModuleUse,
) {
    let unbound_module = |mident: &ModuleIdent| -> Diagnostic {
        diag!(
            NameResolution::UnboundModule,
            (
                mident.loc,
                format!("Invalid 'use'. Unbound module: '{}'", mident),
            )
        )
    };
    macro_rules! add_module_alias {
        ($ident:expr, $alias:expr) => {{
            if let Err(()) = check_restricted_name_all_cases(
                &mut context.defn_context,
                NameCase::ModuleAlias,
                &$alias,
            ) {
                return;
            }

            if let Err(old_loc) = acc.add_module_alias($alias.clone(), $ident) {
                duplicate_module_alias(context, old_loc, $alias)
            }
        }};
    }
    match muse {
        P::ModuleUse::Module(alias_opt) => {
            let mident = module_ident(&mut context.defn_context, in_mident);
            if !context.defn_context.module_members.contains_key(&mident) {
                context.env().add_diag(unbound_module(&mident));
                return;
            };
            let alias = alias_opt
                .map(|m| m.0)
                .unwrap_or_else(|| mident.value.module.0);
            add_module_alias!(mident, alias)
        }
        P::ModuleUse::Members(sub_uses) => {
            let mident = module_ident(&mut context.defn_context, in_mident);
            let members = match context.defn_context.module_members.get(&mident) {
                Some(members) => members,
                None => {
                    context.env().add_diag(unbound_module(&mident));
                    return;
                }
            };
            let mloc = *context
                .defn_context
                .module_members
                .get_loc(&mident)
                .unwrap();
            let sub_uses_kinds = sub_uses
                .into_iter()
                .map(|(member, alia_opt)| {
                    let kind = members.get(&member).cloned();
                    (member, alia_opt, kind)
                })
                .collect::<Vec<_>>();

            for (member, alias_opt, member_kind_opt) in sub_uses_kinds {
                if member.value.as_str() == ModuleName::SELF_NAME {
                    let alias = if let Some(alias) = alias_opt {
                        alias
                    } else {
                        // For Self-inclusion, we respan the symbol to point to Self for better
                        // error messages.
                        let symbol = mident.value.module.0.value;
                        sp(member.loc, symbol)
                    };
                    add_module_alias!(mident, alias);
                    continue;
                }

                // check is member

                let member_kind = match member_kind_opt {
                    None => {
                        let msg = format!(
                            "Invalid 'use'. Unbound member '{}' in module '{}'",
                            member, mident
                        );
                        context.env().add_diag(diag!(
                            NameResolution::UnboundModuleMember,
                            (member.loc, msg),
                            (mloc, format!("Module '{}' declared here", mident)),
                        ));
                        continue;
                    }
                    Some(m) => m,
                };

                let alias = alias_opt.unwrap_or(member);

                let alias = match check_valid_module_member_alias(context, member_kind, alias) {
                    None => continue,
                    Some(alias) => alias,
                };
                if let Err(old_loc) = acc.add_member_alias(alias, mident, member, member_kind) {
                    duplicate_module_member(context, old_loc, alias)
                }
                if matches!(member_kind, ModuleMemberKind::Function) {
                    // remove any previously declared alias to keep in sync with the member alias
                    // map
                    use_funs.implicit.remove(&alias);
                    // not a function declaration
                    let is_public = None;
                    // assume used. We will set it to false if needed when exiting this alias scope
                    let kind = E::ImplicitUseFunKind::UseAlias { used: true };
                    let implicit = E::ImplicitUseFunCandidate {
                        loc: alias.loc,
                        attributes: attributes.clone(),
                        is_public,
                        function: (mident, member),
                        kind,
                    };
                    use_funs.implicit.add(alias, implicit).unwrap();
                }
            }
        }
    }
}

fn use_funs(context: &mut Context, builder: UseFunsBuilder) -> E::UseFuns {
    let UseFunsBuilder {
        explicit: pexplicit,
        implicit,
    } = builder;
    // If None, there was an error and we can skip it
    let explicit = pexplicit
        .into_iter()
        .filter_map(|e| explicit_use_fun(context, e))
        .collect();
    E::UseFuns { explicit, implicit }
}

fn explicit_use_fun(
    context: &mut Context,
    pexplicit: ParserExplicitUseFun,
) -> Option<E::ExplicitUseFun> {
    let ParserExplicitUseFun {
        loc,
        attributes,
        is_public,
        function,
        ty,
        method,
    } = pexplicit;
    let function =
        context.name_access_chain_to_module_access(Access::ApplyPositional, *function)?;
    let ty = context.name_access_chain_to_module_access(Access::Type, *ty)?;
    Some(E::ExplicitUseFun {
        loc,
        attributes,
        is_public,
        function,
        ty,
        method,
    })
}

fn duplicate_module_alias(context: &mut Context, old_loc: Loc, alias: Name) {
    let msg = format!(
        "Duplicate module alias '{}'. Module aliases must be unique within a given namespace",
        alias
    );
    context.env().add_diag(diag!(
        Declarations::DuplicateItem,
        (alias.loc, msg),
        (old_loc, "Alias previously defined here"),
    ));
}

fn duplicate_module_member(context: &mut Context, old_loc: Loc, alias: Name) {
    let msg = format!(
        "Duplicate module member or alias '{}'. Top level names in a namespace must be unique",
        alias
    );
    context.env().add_diag(diag!(
        Declarations::DuplicateItem,
        (alias.loc, msg),
        (old_loc, "Alias previously defined here"),
    ));
}

fn unused_alias(context: &mut Context, _kind: &str, alias: Name) {
    if !context.is_source_definition {
        return;
    }
    let mut diag = diag!(
        UnusedItem::Alias,
        (
            alias.loc,
            format!("Unused 'use' of alias '{}'. Consider removing it", alias)
        ),
    );
    if crate::naming::ast::BuiltinTypeName_::all_names().contains(&alias.value) {
        diag.add_note(format!(
            "This alias does not shadow the built-in type '{}' in type annotations.",
            alias
        ));
    } else if crate::naming::ast::BuiltinFunction_::all_names().contains(&alias.value) {
        diag.add_note(format!(
            "This alias does not shadow the built-in function '{}' in call expressions.",
            alias
        ));
    }
    context.env().add_diag(diag);
}

//**************************************************************************************************
// Structs
//**************************************************************************************************

fn struct_def(
    context: &mut Context,
    structs: &mut UniqueMap<StructName, E::StructDefinition>,
    pstruct: P::StructDefinition,
) {
    let (sname, sdef) = struct_def_(context, structs.len(), pstruct);
    if let Err(_old_loc) = structs.add(sname, sdef) {
        assert!(context.env().has_errors())
    }
}

fn struct_def_(
    context: &mut Context,
    index: usize,
    pstruct: P::StructDefinition,
) -> (StructName, E::StructDefinition) {
    let P::StructDefinition {
        attributes,
        loc,
        name,
        abilities: abilities_vec,
        type_parameters: pty_params,
        fields: pfields,
    } = pstruct;
    let attributes = flatten_attributes(context, AttributePosition::Struct, attributes);
    let warning_filter = warning_filter(context, &attributes);
    context
        .env()
        .add_warning_filter_scope(warning_filter.clone());
    let type_parameters = struct_type_parameters(context, pty_params);
    context.push_type_parameters(type_parameters.iter().map(|tp| &tp.name));
    let abilities = ability_set(context, "modifier", abilities_vec);
    let fields = struct_fields(context, &name, pfields);
    let sdef = E::StructDefinition {
        warning_filter,
        index,
        attributes,
        loc,
        abilities,
        type_parameters,
        fields,
    };
    context.pop_alias_scope(None);
    context.env().pop_warning_filter_scope();
    (name, sdef)
}

fn struct_fields(
    context: &mut Context,
    sname: &StructName,
    pfields: P::StructFields,
) -> E::StructFields {
    let pfields_vec = match pfields {
        P::StructFields::Native(loc) => return E::StructFields::Native(loc),
        P::StructFields::Positional(tys) => {
            let field_tys = tys.into_iter().map(|fty| type_(context, fty)).collect();
            return E::StructFields::Positional(field_tys);
        }
        P::StructFields::Defined(v) => v,
    };
    let mut field_map = UniqueMap::new();
    for (idx, (field, pt)) in pfields_vec.into_iter().enumerate() {
        let t = type_(context, pt);
        if let Err((field, old_loc)) = field_map.add(field, (idx, t)) {
            context.env().add_diag(diag!(
                Declarations::DuplicateItem,
                (
                    field.loc(),
                    format!(
                        "Duplicate definition for field '{}' in struct '{}'",
                        field, sname
                    ),
                ),
                (old_loc, "Field previously defined here"),
            ));
        }
    }
    E::StructFields::Named(field_map)
}

//**************************************************************************************************
// Friends
//**************************************************************************************************

fn friend(
    context: &mut Context,
    friends: &mut UniqueMap<ModuleIdent, E::Friend>,
    pfriend: P::FriendDecl,
) {
    match friend_(context, pfriend) {
        Some((mident, friend)) => match friends.get(&mident) {
            None => friends.add(mident, friend).unwrap(),
            Some(old_friend) => {
                let msg = format!(
                    "Duplicate friend declaration '{}'. Friend declarations in a module must be \
                     unique",
                    mident
                );
                context.env().add_diag(diag!(
                    Declarations::DuplicateItem,
                    (friend.loc, msg),
                    (old_friend.loc, "Friend previously declared here"),
                ));
            }
        },
        None => assert!(context.env().has_errors()),
    };
}

fn friend_(context: &mut Context, pfriend_decl: P::FriendDecl) -> Option<(ModuleIdent, E::Friend)> {
    let P::FriendDecl {
        attributes: pattributes,
        loc,
        friend: pfriend,
    } = pfriend_decl;
    let mident = context.name_access_chain_to_module_ident(pfriend)?;
    let attr_locs = pattributes
        .iter()
        .map(|sp!(loc, _)| *loc)
        .collect::<Vec<_>>();
    let attributes = flatten_attributes(context, AttributePosition::Friend, pattributes);
    Some((
        mident,
        E::Friend {
            attributes,
            attr_locs,
            loc,
        },
    ))
}

//**************************************************************************************************
// Constants
//**************************************************************************************************

fn constant(
    context: &mut Context,
    constants: &mut UniqueMap<ConstantName, E::Constant>,
    pconstant: P::Constant,
) {
    let (name, constant) = constant_(context, constants.len(), pconstant);
    if let Err(_old_loc) = constants.add(name, constant) {
        assert!(context.env().has_errors())
    }
}

fn constant_(
    context: &mut Context,
    index: usize,
    pconstant: P::Constant,
) -> (ConstantName, E::Constant) {
    let P::Constant {
        attributes: pattributes,
        loc,
        name,
        signature: psignature,
        value: pvalue,
    } = pconstant;
    let attributes = flatten_attributes(context, AttributePosition::Constant, pattributes);
    let warning_filter = warning_filter(context, &attributes);
    context
        .env()
        .add_warning_filter_scope(warning_filter.clone());
    let signature = type_(context, psignature);
    let value = *exp(context, Box::new(pvalue));
    let constant = E::Constant {
        warning_filter,
        index,
        attributes,
        loc,
        signature,
        value,
    };
    context.env().pop_warning_filter_scope();
    (name, constant)
}

//**************************************************************************************************
// Functions
//**************************************************************************************************

fn function(
    context: &mut Context,
    module_and_use_funs: Option<(ModuleIdent, &mut UseFunsBuilder)>,
    functions: &mut UniqueMap<FunctionName, E::Function>,
    pfunction: P::Function,
) {
    let (fname, fdef) = function_(context, module_and_use_funs, functions.len(), pfunction);
    if let Err(_old_loc) = functions.add(fname, fdef) {
        assert!(context.env().has_errors())
    }
}

fn function_(
    context: &mut Context,
    module_and_use_funs: Option<(ModuleIdent, &mut UseFunsBuilder)>,
    index: usize,
    pfunction: P::Function,
) -> (FunctionName, E::Function) {
    let P::Function {
        attributes: pattributes,
        loc,
        name,
        visibility: pvisibility,
        entry,
        macro_,
        signature: psignature,
        body: pbody,
    } = pfunction;
    let attributes = flatten_attributes(context, AttributePosition::Function, pattributes);
    let warning_filter = warning_filter(context, &attributes);
    context
        .env()
        .add_warning_filter_scope(warning_filter.clone());
    if let (Some(entry_loc), Some(macro_loc)) = (entry, macro_) {
        let e_msg = format!(
            "Invalid function declaration. \
            It is meaningless for '{MACRO_MODIFIER}' functions to be '{ENTRY_MODIFIER}' since they \
            are fully-expanded inline during compilation"
        );
        let m_msg = format!("Function declared as '{MACRO_MODIFIER}' here");
        context.env().add_diag(diag!(
            Declarations::InvalidFunction,
            (entry_loc, e_msg),
            (macro_loc, m_msg),
        ));
    }
    if let (Some(macro_loc), sp!(native_loc, P::FunctionBody_::Native)) = (macro_, &pbody) {
        let n_msg = format!(
            "Invalid function declaration. \
            '{NATIVE_MODIFIER}' functions cannot be '{MACRO_MODIFIER}'",
        );
        let m_msg = format!("Function declared as '{MACRO_MODIFIER}' here");
        context.env().add_diag(diag!(
            Declarations::InvalidFunction,
            (*native_loc, n_msg),
            (macro_loc, m_msg),
        ));
    }
    if let Some(macro_loc) = macro_ {
        let current_package = context.current_package();
        context
            .env()
            .check_feature(current_package, FeatureGate::MacroFuns, macro_loc);
    }
    let visibility = visibility(pvisibility);
    let signature = function_signature(context, macro_, psignature);
    let body = function_body(context, pbody);
    if let Some((m, use_funs_builder)) = module_and_use_funs {
        let implicit = E::ImplicitUseFunCandidate {
            loc: name.loc(),
            attributes: attributes.clone(),
            is_public: Some(visibility.loc().unwrap_or_else(|| name.loc())),
            function: (m, name.0),
            // disregard used/unused information tracking
            kind: E::ImplicitUseFunKind::FunctionDeclaration,
        };
        // we can ignore any error, since the alias map will catch conflicting names
        let _ = use_funs_builder.implicit.add(name.0, implicit);
    }
    let fdef = E::Function {
        warning_filter,
        index,
        attributes,
        loc,
        visibility,
        entry,
        macro_,
        signature,
        body,
    };
    context.pop_alias_scope(None);
    context.env().pop_warning_filter_scope();
    (name, fdef)
}

fn visibility(pvisibility: P::Visibility) -> E::Visibility {
    match pvisibility {
        P::Visibility::Friend(loc) => E::Visibility::Friend(loc),
        P::Visibility::Internal => E::Visibility::Internal,
        P::Visibility::Package(loc) => E::Visibility::Package(loc),
        P::Visibility::Public(loc) => E::Visibility::Public(loc),
    }
}

fn function_signature(
    context: &mut Context,
    is_macro: Option<Loc>,
    psignature: P::FunctionSignature,
) -> E::FunctionSignature {
    let P::FunctionSignature {
        type_parameters: pty_params,
        parameters: pparams,
        return_type: pret_ty,
    } = psignature;
    let type_parameters = function_type_parameters(context, is_macro, pty_params);
    context.push_type_parameters(type_parameters.iter().map(|(name, _)| name));
    let parameters = pparams
        .into_iter()
        .map(|(pmut, v, t)| (mutability(context, v.loc(), pmut), v, type_(context, t)))
        .collect::<Vec<_>>();
    for (_, v, _) in &parameters {
        check_valid_function_parameter_name(context, is_macro, v)
    }
    let return_type = type_(context, pret_ty);
    E::FunctionSignature {
        type_parameters,
        parameters,
        return_type,
    }
}

fn function_body(context: &mut Context, sp!(loc, pbody_): P::FunctionBody) -> E::FunctionBody {
    use E::FunctionBody_ as EF;
    use P::FunctionBody_ as PF;
    let body_ = match pbody_ {
        PF::Native => EF::Native,
        PF::Defined(seq) => EF::Defined(sequence(context, loc, seq)),
    };
    sp(loc, body_)
}

//**************************************************************************************************
// Types
//**************************************************************************************************

fn ability_set(context: &mut Context, case: &str, abilities_vec: Vec<Ability>) -> E::AbilitySet {
    let mut set = E::AbilitySet::empty();
    for ability in abilities_vec {
        let loc = ability.loc;
        if let Err(prev_loc) = set.add(ability) {
            context.env().add_diag(diag!(
                Declarations::DuplicateItem,
                (loc, format!("Duplicate '{}' ability {}", ability, case)),
                (prev_loc, "Ability previously given here")
            ));
        }
    }
    set
}

fn function_type_parameters(
    context: &mut Context,
    is_macro: Option<Loc>,
    pty_params: Vec<(Name, Vec<Ability>)>,
) -> Vec<(Name, E::AbilitySet)> {
    pty_params
        .into_iter()
        .map(|(name, constraints_vec)| {
            let constraints = ability_set(context, "constraint", constraints_vec);
            let _ = check_valid_type_parameter_name(context, is_macro, &name);
            (name, constraints)
        })
        .collect()
}

fn struct_type_parameters(
    context: &mut Context,
    pty_params: Vec<P::StructTypeParameter>,
) -> Vec<E::StructTypeParameter> {
    pty_params
        .into_iter()
        .map(|param| {
            let _ = check_valid_type_parameter_name(context, None, &param.name);
            E::StructTypeParameter {
                is_phantom: param.is_phantom,
                name: param.name,
                constraints: ability_set(context, "constraint", param.constraints),
            }
        })
        .collect()
}

fn type_(context: &mut Context, sp!(loc, pt_): P::Type) -> E::Type {
    use E::Type_ as ET;
    use P::Type_ as PT;
    let t_ = match pt_ {
        PT::Unit => ET::Unit,
        PT::Multiple(ts) => ET::Multiple(types(context, ts)),
        PT::Apply(pn, ptyargs) => {
            let tyargs = types(context, ptyargs);
            match context.name_access_chain_to_module_access(Access::Type, *pn) {
                None => {
                    assert!(context.env().has_errors());
                    ET::UnresolvedError
                }
                Some(n) => ET::Apply(n, tyargs),
            }
        }
        PT::Ref(mut_, inner) => ET::Ref(mut_, Box::new(type_(context, *inner))),
        PT::Fun(args, result) => {
            let args = types(context, args);
            let result = type_(context, *result);
            ET::Fun(args, Box::new(result))
        }
        PT::UnresolvedError => ET::UnresolvedError,
    };
    sp(loc, t_)
}

fn types(context: &mut Context, pts: Vec<P::Type>) -> Vec<E::Type> {
    pts.into_iter().map(|pt| type_(context, pt)).collect()
}

fn optional_types(context: &mut Context, pts_opt: Option<Vec<P::Type>>) -> Option<Vec<E::Type>> {
    pts_opt.map(|pts| pts.into_iter().map(|pt| type_(context, pt)).collect())
}

//**************************************************************************************************
// Expressions
//**************************************************************************************************

#[growing_stack]
fn sequence(context: &mut Context, loc: Loc, seq: P::Sequence) -> E::Sequence {
    let (puses, pitems, maybe_last_semicolon_loc, pfinal_item) = seq;

    let (new_scope, use_funs_builder) = uses(context, puses);
    context.push_alias_scope(loc, new_scope);
    let mut use_funs = use_funs(context, use_funs_builder);
    let mut items: VecDeque<E::SequenceItem> = pitems
        .into_iter()
        .map(|item| sequence_item(context, item))
        .collect();
    let final_e_opt = pfinal_item.map(|item| exp(context, Box::new(item)));
    let final_e = match final_e_opt {
        None => {
            let last_semicolon_loc = match maybe_last_semicolon_loc {
                Some(l) => l,
                None => loc,
            };
            Box::new(sp(last_semicolon_loc, E::Exp_::Unit { trailing: true }))
        }
        Some(e) => e,
    };
    let final_item = sp(final_e.loc, E::SequenceItem_::Seq(final_e));
    items.push_back(final_item);
    context.pop_alias_scope(Some(&mut use_funs));
    (use_funs, items)
}

#[growing_stack]
fn sequence_item(context: &mut Context, sp!(loc, pitem_): P::SequenceItem) -> E::SequenceItem {
    use E::SequenceItem_ as ES;
    use P::SequenceItem_ as PS;
    let item_ = match pitem_ {
        PS::Seq(e) => ES::Seq(exp(context, e)),
        PS::Declare(pb, pty_opt) => {
            let b_opt = bind_list(context, pb);
            let ty_opt = pty_opt.map(|t| type_(context, t));
            match b_opt {
                None => {
                    assert!(context.env().has_errors());
                    ES::Seq(Box::new(sp(loc, E::Exp_::UnresolvedError)))
                }
                Some(b) => ES::Declare(b, ty_opt),
            }
        }
        PS::Bind(pb, pty_opt, pe) => {
            let b_opt = bind_list(context, pb);
            let ty_opt = pty_opt.map(|t| type_(context, t));
            let e_ = exp(context, pe);
            let e = match ty_opt {
                None => e_,
                Some(ty) => Box::new(sp(e_.loc, E::Exp_::Annotate(e_, ty))),
            };
            match b_opt {
                None => {
                    assert!(context.env().has_errors());
                    ES::Seq(Box::new(sp(loc, E::Exp_::UnresolvedError)))
                }
                Some(b) => ES::Bind(b, e),
            }
        }
    };
    sp(loc, item_)
}

fn exps(context: &mut Context, pes: Vec<P::Exp>) -> Vec<E::Exp> {
    pes.into_iter()
        .map(|pe| *exp(context, Box::new(pe)))
        .collect()
}

#[growing_stack]
fn exp(context: &mut Context, pe: Box<P::Exp>) -> Box<E::Exp> {
    use E::Exp_ as EE;
    use P::Exp_ as PE;
    let sp!(loc, pe_) = *pe;
    macro_rules! unwrap_or_error_exp {
        ($opt_exp:expr) => {
            if let Some(value) = $opt_exp {
                value
            } else {
                assert!(context.env().has_errors());
                EE::UnresolvedError
            }
        };
    }
    let e_ = match pe_ {
        PE::Unit => EE::Unit { trailing: false },
        PE::Parens(pe) => {
            match *pe {
                sp!(pe_loc, PE::Cast(plhs, pty)) => {
                    let e_ = exp_cast(context, /* in_parens */ true, plhs, pty);
                    return Box::new(sp(pe_loc, e_));
                }
                pe => return exp(context, Box::new(pe)),
            }
        }
        PE::Value(pv) => unwrap_or_error_exp!(value(&mut context.defn_context, pv).map(EE::Value)),
        PE::Name(_, Some(_)) => {
            let msg = "Expected name to be followed by a brace-enclosed list of field expressions \
                or a parenthesized list of arguments for a function call";
            context
                .env()
                .add_diag(diag!(NameResolution::NamePositionMismatch, (loc, msg)));
            EE::UnresolvedError
        }
        PE::Name(pn, ptys_opt) => {
            let en_opt = context.name_access_chain_to_module_access(Access::Term, pn);
            let tys_opt = optional_types(context, ptys_opt);
            unwrap_or_error_exp!(en_opt.map(|en| EE::Name(en, tys_opt)))
        }
        PE::Call(pn, is_macro, ptys_opt, sp!(rloc, prs)) => {
            let tys_opt = optional_types(context, ptys_opt);
            let ers = sp(rloc, exps(context, prs));
            let en_opt = context.name_access_chain_to_module_access(Access::ApplyPositional, pn);
            unwrap_or_error_exp!(en_opt.map(|en| EE::Call(en, is_macro, tys_opt, ers)))
        }
        PE::Pack(pn, ptys_opt, pfields) => {
            let en_opt = context.name_access_chain_to_module_access(Access::ApplyNamed, pn);
            let tys_opt = optional_types(context, ptys_opt);
            let efields_vec = pfields
                .into_iter()
                .map(|(f, pe)| (f, *exp(context, Box::new(pe))))
                .collect();
            let efields = named_fields(context, loc, "construction", "argument", efields_vec);
            unwrap_or_error_exp!(en_opt.map(|en| EE::Pack(en, tys_opt, efields)))
        }
        PE::Vector(vec_loc, ptys_opt, sp!(args_loc, pargs_)) => {
            let tys_opt = optional_types(context, ptys_opt);
            let args = sp(args_loc, exps(context, pargs_));
            EE::Vector(vec_loc, tys_opt, args)
        }
        PE::IfElse(pb, pt, pf_opt) => {
            let eb = exp(context, pb);
            let et = exp(context, pt);
            let ef = match pf_opt {
                None => Box::new(sp(loc, EE::Unit { trailing: false })),
                Some(pf) => exp(context, pf),
            };
            EE::IfElse(eb, et, ef)
        }
        PE::Labeled(name, pe) => {
            let e = exp(context, pe);
            return maybe_labeled_exp(context, loc, name, e);
        }
        PE::While(pb, ploop) => EE::While(None, exp(context, pb), exp(context, ploop)),
        PE::Loop(ploop) => EE::Loop(None, exp(context, ploop)),
        PE::Block(seq) => EE::Block(None, sequence(context, loc, seq)),
        PE::Lambda(plambda, pty_opt, pe) => {
            let elambda_opt = lambda_bind_list(context, plambda);
            let ty_opt = pty_opt.map(|t| type_(context, t));
            match elambda_opt {
                Some(elambda) => EE::Lambda(elambda, ty_opt, exp(context, pe)),
                None => {
                    assert!(context.env().has_errors());
                    EE::UnresolvedError
                }
            }
        }
        PE::Quant(..) => {
            context.spec_deprecated(loc, /* is_error */ true);
            EE::UnresolvedError
        }
        PE::ExpList(pes) => {
            assert!(pes.len() > 1);
            EE::ExpList(exps(context, pes))
        }

        PE::Assign(lvalue, rhs) => {
            let l_opt = lvalues(context, lvalue);
            let er = exp(context, rhs);
            match l_opt {
                None => {
                    assert!(context.env().has_errors());
                    EE::UnresolvedError
                }
                Some(LValue::Assigns(al)) => EE::Assign(al, er),
                Some(LValue::Mutate(el)) => EE::Mutate(el, er),
                Some(LValue::FieldMutate(edotted)) => EE::FieldMutate(edotted, er),
            }
        }
        PE::Abort(pe) => EE::Abort(exp(context, pe)),
        PE::Return(name_opt, pe_opt) => {
            let ev = match pe_opt {
                None => Box::new(sp(loc, EE::Unit { trailing: false })),
                Some(pe) => exp(context, pe),
            };
            EE::Return(name_opt, ev)
        }
        PE::Break(name_opt, pe_opt) => {
            let ev = match pe_opt {
                None => Box::new(sp(loc, EE::Unit { trailing: false })),
                Some(pe) => exp(context, pe),
            };
            EE::Break(name_opt, ev)
        }
        PE::Continue(name) => EE::Continue(name),
        PE::Dereference(pe) => EE::Dereference(exp(context, pe)),
        PE::UnaryExp(op, pe) => EE::UnaryExp(op, exp(context, pe)),
        PE::BinopExp(_pl, op, _pr) if op.value.is_spec_only() => {
            context.spec_deprecated(loc, /* is_error */ true);
            EE::UnresolvedError
        }
        e_ @ PE::BinopExp(..) => {
            process_binops!(
                (P::BinOp, Loc),
                Box<E::Exp>,
                Box::new(sp(loc, e_)),
                e,
                *e,
                sp!(loc, PE::BinopExp(lhs, op, rhs)) => { (lhs, (op, loc), rhs) },
                { exp(context, e) },
                value_stack,
                (bop, loc) => {
                    let el = value_stack.pop().expect("ICE binop expansion issue");
                    let er = value_stack.pop().expect("ICE binop expansion issue");
                    Box::new(sp(loc, EE::BinopExp(el, bop, er)))
                }
            )
            .value
        }
        PE::Move(loc, pdotted) => move_or_copy_path(context, PathCase::Move(loc), pdotted),
        PE::Copy(loc, pdotted) => move_or_copy_path(context, PathCase::Copy(loc), pdotted),
        PE::Borrow(mut_, pdotted) => match exp_dotted(context, pdotted) {
            Some(edotted) => EE::ExpDotted(E::DottedUsage::Borrow(mut_), edotted),
            None => {
                assert!(context.env().has_errors());
                EE::UnresolvedError
            }
        },
        pdotted_ @ PE::Dot(_, _) => match exp_dotted(context, Box::new(sp(loc, pdotted_))) {
            Some(edotted) => EE::ExpDotted(E::DottedUsage::Use, edotted),
            None => {
                assert!(context.env().has_errors());
                EE::UnresolvedError
            }
        },

        pdotted_ @ PE::Index(_, _) => {
            let cur_pkg = context.current_package();
            let supports_paths = context
                .env()
                .supports_feature(cur_pkg, FeatureGate::Move2024Paths);
            let supports_syntax_methods = context
                .env()
                .supports_feature(cur_pkg, FeatureGate::SyntaxMethods);
            if !supports_paths || !supports_syntax_methods {
                let mut diag = context.spec_deprecated_diag(loc, /* is_error */ true);
                let valid_editions =
                    editions::valid_editions_for_feature(FeatureGate::SyntaxMethods)
                        .into_iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                diag.add_note(format!(
                    "If this was intended to be a 'syntax' index call, \
                    consider updating your Move edition to '{valid_editions}'"
                ));
                diag.add_note(editions::UPGRADE_NOTE);
                context.env().add_diag(diag);
                EE::UnresolvedError
            } else {
                match exp_dotted(context, Box::new(sp(loc, pdotted_))) {
                    Some(edotted) => EE::ExpDotted(E::DottedUsage::Use, edotted),
                    None => {
                        assert!(context.env().has_errors());
                        EE::UnresolvedError
                    }
                }
            }
        }

        PE::DotCall(pdotted, n, is_macro, ptys_opt, sp!(rloc, prs)) => {
            match exp_dotted(context, pdotted) {
                Some(edotted) => {
                    let pkg = context.current_package();
                    context.env().check_feature(pkg, FeatureGate::DotCall, loc);
                    let tys_opt = optional_types(context, ptys_opt);
                    let ers = sp(rloc, exps(context, prs));
                    EE::MethodCall(edotted, n, is_macro, tys_opt, ers)
                }
                None => {
                    assert!(context.env().has_errors());
                    EE::UnresolvedError
                }
            }
        }
        PE::Cast(e, ty) => exp_cast(context, /* in_parens */ false, e, ty),
        PE::Annotate(e, ty) => EE::Annotate(exp(context, e), type_(context, ty)),
        PE::Spec(_) => {
            context.spec_deprecated(loc, /* is_error */ false);
            EE::Unit { trailing: false }
        }
        PE::UnresolvedError => EE::UnresolvedError,
    };
    Box::new(sp(loc, e_))
}

fn exp_cast(context: &mut Context, in_parens: bool, plhs: Box<P::Exp>, pty: P::Type) -> E::Exp_ {
    use E::Exp_ as EE;
    use P::Exp_ as PE;
    fn ambiguous_cast(e: &P::Exp) -> bool {
        match &e.value {
            PE::Value(_)
            | PE::Move(_, _)
            | PE::Copy(_, _)
            | PE::Name(_, _)
            | PE::Call(_, _, _, _)
            | PE::Pack(_, _, _)
            | PE::Vector(_, _, _)
            | PE::Block(_)
            | PE::ExpList(_)
            | PE::Unit
            | PE::Parens(_)
            | PE::Annotate(_, _)
            | PE::UnresolvedError => false,

            PE::IfElse(_, _, _)
            | PE::While(_, _)
            | PE::Loop(_)
            | PE::Labeled(_, _)
            | PE::Lambda(_, _, _)
            | PE::Quant(_, _, _, _, _)
            | PE::Assign(_, _)
            | PE::Abort(_)
            | PE::Return(_, _)
            | PE::Break(_, _)
            | PE::Continue(_)
            | PE::UnaryExp(_, _)
            | PE::BinopExp(_, _, _)
            | PE::Cast(_, _)
            | PE::Spec(_) => true,

            PE::DotCall(lhs, _, _, _, _)
            | PE::Dot(lhs, _)
            | PE::Index(lhs, _)
            | PE::Borrow(_, lhs)
            | PE::Dereference(lhs) => ambiguous_cast(lhs),
        }
    }
    if !in_parens {
        let current_package = context.current_package();
        let loc = plhs.loc;
        let supports_feature =
            context
                .env()
                .check_feature(current_package, FeatureGate::NoParensCast, loc);
        if supports_feature && ambiguous_cast(&plhs) {
            let msg = "Potentially ambiguous 'as'. Add parentheses to disambiguate";
            context
                .env()
                .add_diag(diag!(Syntax::AmbiguousCast, (loc, msg)));
        }
    }
    EE::Cast(exp(context, plhs), type_(context, pty))
}

// If the expression can take a label, attach the label. Otherwise error
fn maybe_labeled_exp(
    context: &mut Context,
    loc: Loc,
    label: BlockLabel,
    e: Box<E::Exp>,
) -> Box<E::Exp> {
    let sp!(_eloc, e_) = *e;
    let e_ = match e_ {
        E::Exp_::While(label_opt, cond, body) => {
            ensure_unique_label(context, loc, &label, label_opt);
            E::Exp_::While(Some(label), cond, body)
        }
        E::Exp_::Loop(label_opt, body) => {
            ensure_unique_label(context, loc, &label, label_opt);
            E::Exp_::Loop(Some(label), body)
        }
        E::Exp_::Block(label_opt, seq) => {
            ensure_unique_label(context, loc, &label, label_opt);
            E::Exp_::Block(Some(label), seq)
        }
        _ => {
            let msg = "Invalid label. Labels can only be used on 'while', 'loop', or block '{{}}' \
                 expressions";
            context
                .env()
                .add_diag(diag!(Syntax::InvalidLabel, (loc, msg)));
            E::Exp_::UnresolvedError
        }
    };
    Box::new(sp(loc, e_))
}

fn ensure_unique_label(
    context: &mut Context,
    loc: Loc,
    _label: &BlockLabel,
    label_opt: Option<BlockLabel>,
) {
    if let Some(old_label) = label_opt {
        context.env().add_diag(diag!(
            Syntax::InvalidLabel,
            (loc, "Multiple labels for a single expression"),
            (old_label.0.loc, "Label previously given here"),
        ));
    }
}

#[derive(Copy, Clone)]
enum PathCase {
    Move(Loc),
    Copy(Loc),
}

impl PathCase {
    fn loc(self) -> Loc {
        match self {
            PathCase::Move(loc) | PathCase::Copy(loc) => loc,
        }
    }
    fn case(self) -> &'static str {
        match self {
            PathCase::Move(_) => "move",
            PathCase::Copy(_) => "copy",
        }
    }
}

fn move_or_copy_path(context: &mut Context, case: PathCase, pe: Box<P::Exp>) -> E::Exp_ {
    match move_or_copy_path_(context, case, pe) {
        Some(e) => e,
        None => {
            assert!(context.env().has_errors());
            E::Exp_::UnresolvedError
        }
    }
}

fn move_or_copy_path_(context: &mut Context, case: PathCase, pe: Box<P::Exp>) -> Option<E::Exp_> {
    let e = exp_dotted(context, pe)?;
    let cloc = case.loc();
    match &e.value {
        E::ExpDotted_::Exp(inner) => {
            if !matches!(&inner.value, E::Exp_::Name(_, _)) {
                let cmsg = format!("Invalid '{}' of expression", case.case());
                let emsg = "Expected a name or path access, e.g. 'x' or 'e.f'";
                context.env().add_diag(diag!(
                    Syntax::InvalidMoveOrCopy,
                    (cloc, cmsg),
                    (inner.loc, emsg)
                ));
                return None;
            }
        }
        E::ExpDotted_::Dot(_, _) | E::ExpDotted_::Index(_, _) => {
            let current_package = context.current_package();
            context
                .env()
                .check_feature(current_package, FeatureGate::Move2024Paths, cloc);
        }
    }
    Some(match case {
        PathCase::Move(loc) => E::Exp_::ExpDotted(E::DottedUsage::Move(loc), e),
        PathCase::Copy(loc) => E::Exp_::ExpDotted(E::DottedUsage::Copy(loc), e),
    })
}

#[growing_stack]
fn exp_dotted(context: &mut Context, pdotted: Box<P::Exp>) -> Option<Box<E::ExpDotted>> {
    use E::ExpDotted_ as EE;
    use P::Exp_ as PE;
    let sp!(loc, pdotted_) = *pdotted;
    let edotted_ = match pdotted_ {
        PE::Dot(plhs, field) => {
            let lhs = exp_dotted(context, plhs)?;
            EE::Dot(lhs, field)
        }
        PE::Index(plhs, sp!(argloc, args)) => {
            let cur_pkg = context.current_package();
            context
                .env()
                .check_feature(cur_pkg, FeatureGate::Move2024Paths, loc);
            context
                .env()
                .check_feature(cur_pkg, FeatureGate::SyntaxMethods, loc);
            let lhs = exp_dotted(context, plhs)?;
            let args = args
                .into_iter()
                .map(|arg| *exp(context, Box::new(arg)))
                .collect::<Vec<_>>();
            EE::Index(lhs, sp(argloc, args))
        }
        pe_ => EE::Exp(exp(context, Box::new(sp(loc, pe_)))),
    };
    Some(Box::new(sp(loc, edotted_)))
}

fn value(context: &mut DefnContext, sp!(loc, pvalue_): P::Value) -> Option<E::Value> {
    use E::Value_ as EV;
    use P::Value_ as PV;
    let value_ = match pvalue_ {
        PV::Address(addr) => {
            let addr = top_level_address(context, /* suggest_declaration */ true, addr);
            EV::Address(addr)
        }
        PV::Num(s) if s.ends_with("u8") => match parse_u8(&s[..s.len() - 2]) {
            Ok((u, _format)) => EV::U8(u),
            Err(_) => {
                context.env.add_diag(num_too_big_error(loc, "'u8'"));
                return None;
            }
        },
        PV::Num(s) if s.ends_with("u16") => match parse_u16(&s[..s.len() - 3]) {
            Ok((u, _format)) => EV::U16(u),
            Err(_) => {
                context.env.add_diag(num_too_big_error(loc, "'u16'"));
                return None;
            }
        },
        PV::Num(s) if s.ends_with("u32") => match parse_u32(&s[..s.len() - 3]) {
            Ok((u, _format)) => EV::U32(u),
            Err(_) => {
                context.env.add_diag(num_too_big_error(loc, "'u32'"));
                return None;
            }
        },
        PV::Num(s) if s.ends_with("u64") => match parse_u64(&s[..s.len() - 3]) {
            Ok((u, _format)) => EV::U64(u),
            Err(_) => {
                context.env.add_diag(num_too_big_error(loc, "'u64'"));
                return None;
            }
        },
        PV::Num(s) if s.ends_with("u128") => match parse_u128(&s[..s.len() - 4]) {
            Ok((u, _format)) => EV::U128(u),
            Err(_) => {
                context.env.add_diag(num_too_big_error(loc, "'u128'"));
                return None;
            }
        },
        PV::Num(s) if s.ends_with("u256") => match parse_u256(&s[..s.len() - 4]) {
            Ok((u, _format)) => EV::U256(u),
            Err(_) => {
                context.env.add_diag(num_too_big_error(loc, "'u256'"));
                return None;
            }
        },

        PV::Num(s) => match parse_u256(&s) {
            Ok((u, _format)) => EV::InferredNum(u),
            Err(_) => {
                context.env.add_diag(num_too_big_error(
                    loc,
                    "the largest possible integer type, 'u256'",
                ));
                return None;
            }
        },
        PV::Bool(b) => EV::Bool(b),
        PV::HexString(s) => match hex_string::decode(loc, &s) {
            Ok(v) => EV::Bytearray(v),
            Err(e) => {
                context.env.add_diag(*e);
                return None;
            }
        },
        PV::ByteString(s) => match byte_string::decode(loc, &s) {
            Ok(v) => EV::Bytearray(v),
            Err(e) => {
                context.env.add_diags(e);
                return None;
            }
        },
    };
    Some(sp(loc, value_))
}

// Create an error for an integer literal that is too big to fit in its type.
// This assumes that the literal is the current token.
fn num_too_big_error(loc: Loc, type_description: &'static str) -> Diagnostic {
    diag!(
        Syntax::InvalidNumber,
        (
            loc,
            format!(
                "Invalid number literal. The given literal is too large to fit into {}",
                type_description
            )
        ),
    )
}

//**************************************************************************************************
// Fields
//**************************************************************************************************

fn named_fields<T>(
    context: &mut Context,
    loc: Loc,
    case: &str,
    verb: &str,
    xs: Vec<(Field, T)>,
) -> Fields<T> {
    let mut fmap = UniqueMap::new();
    for (idx, (field, x)) in xs.into_iter().enumerate() {
        if let Err((field, old_loc)) = fmap.add(field, (idx, x)) {
            context.env().add_diag(diag!(
                Declarations::DuplicateItem,
                (loc, format!("Invalid {}", case)),
                (
                    field.loc(),
                    format!("Duplicate {} given for field '{}'", verb, field),
                ),
                (old_loc, "Field previously defined here".into()),
            ))
        }
    }
    fmap
}

//**************************************************************************************************
// LValues
//**************************************************************************************************

fn bind_list(context: &mut Context, sp!(loc, pbs_): P::BindList) -> Option<E::LValueList> {
    let bs_: Option<Vec<E::LValue>> = pbs_.into_iter().map(|pb| bind(context, pb)).collect();
    Some(sp(loc, bs_?))
}

fn bind(context: &mut Context, sp!(loc, pb_): P::Bind) -> Option<E::LValue> {
    use E::LValue_ as EL;
    use P::Bind_ as PB;
    let b_ = match pb_ {
        PB::Var(pmut, v) => {
            let emut = mutability(context, v.loc(), pmut);
            check_valid_local_name(context, &v);
            EL::Var(Some(emut), sp(loc, E::ModuleAccess_::Name(v.0)), None)
        }
        PB::Unpack(ptn, ptys_opt, pfields) => {
            let tn = context.name_access_chain_to_module_access(Access::ApplyNamed, *ptn)?;
            let tys_opt = optional_types(context, ptys_opt);
            let fields = match pfields {
                FieldBindings::Named(named_bindings) => {
                    let vfields: Option<Vec<(Field, E::LValue)>> = named_bindings
                        .into_iter()
                        .map(|(f, pb)| Some((f, bind(context, pb)?)))
                        .collect();
                    let fields =
                        named_fields(context, loc, "deconstruction binding", "binding", vfields?);
                    E::FieldBindings::Named(fields)
                }
                FieldBindings::Positional(positional_bindings) => {
                    let fields: Option<Vec<E::LValue>> = positional_bindings
                        .into_iter()
                        .map(|b| bind(context, b))
                        .collect();
                    E::FieldBindings::Positional(fields?)
                }
            };
            EL::Unpack(tn, tys_opt, fields)
        }
    };
    Some(sp(loc, b_))
}

fn lambda_bind_list(
    context: &mut Context,
    sp!(loc, plambda): P::LambdaBindings,
) -> Option<E::LambdaLValues> {
    let elambda = plambda
        .into_iter()
        .map(|(pbs, ty_opt)| {
            let bs = bind_list(context, pbs)?;
            let ety = ty_opt.map(|t| type_(context, t));
            Some((bs, ety))
        })
        .collect::<Option<_>>()?;
    Some(sp(loc, elambda))
}

enum LValue {
    Assigns(E::LValueList),
    FieldMutate(Box<E::ExpDotted>),
    Mutate(Box<E::Exp>),
}

fn lvalues(context: &mut Context, e: Box<P::Exp>) -> Option<LValue> {
    use LValue as L;
    use P::Exp_ as PE;
    let sp!(loc, e_) = *e;
    let al: LValue = match e_ {
        PE::Unit => L::Assigns(sp(loc, vec![])),
        PE::ExpList(pes) => {
            let al_opt: Option<E::LValueList_> =
                pes.into_iter().map(|pe| assign(context, pe)).collect();
            L::Assigns(sp(loc, al_opt?))
        }
        PE::Dereference(pr) => {
            let er = exp(context, pr);
            L::Mutate(er)
        }
        pdotted_ @ PE::Dot(_, _) => {
            let dotted = exp_dotted(context, Box::new(sp(loc, pdotted_)))?;
            L::FieldMutate(dotted)
        }
        PE::Index(_, _) => {
            context.env().add_diag(diag!(
                Syntax::InvalidLValue,
                (
                    loc,
                    "Index syntax it not yet supported in left-hand positions"
                )
            ));
            return None;
        }
        _ => L::Assigns(sp(loc, vec![assign(context, sp(loc, e_))?])),
    };
    Some(al)
}

fn assign(context: &mut Context, sp!(loc, e_): P::Exp) -> Option<E::LValue> {
    use E::LValue_ as EL;
    use E::ModuleAccess_ as M;
    use P::Exp_ as PE;
    match e_ {
        PE::Name(name, ptys_opt) => {
            let resolved_name =
                context.name_access_chain_to_module_access(Access::Term, name.clone());
            match resolved_name {
                Some(sp!(_, M::Name(_))) if ptys_opt.is_some() => {
                    let msg = "Unexpected assignment of instantiated type without fields";
                    let mut diag = diag!(Syntax::InvalidLValue, (loc, msg));
                    diag.add_note(format!(
                        "If you are trying to unpack a struct, try adding fields, e.g.'{} {{}}'",
                        name
                    ));
                    context.env().add_diag(diag);
                    None
                }
                Some(sp!(_, M::ModuleAccess(_, _))) => {
                    let msg = "Unexpected assignment of module access without fields";
                    let mut diag = diag!(Syntax::InvalidLValue, (loc, msg));
                    diag.add_note(format!(
                        "If you are trying to unpack a struct, try adding fields, e.g.'{} {{}}'",
                        name
                    ));
                    context.env().add_diag(diag);
                    None
                }
                Some(sp!(_, name @ M::Name(_))) => {
                    Some(sp(loc, EL::Var(None, sp(loc, name), None)))
                }
                None => None,
            }
        }
        PE::Pack(pn, ptys_opt, pfields) => {
            let en = context.name_access_chain_to_module_access(Access::ApplyNamed, pn)?;
            let tys_opt = optional_types(context, ptys_opt);
            let efields = assign_unpack_fields(context, loc, pfields)?;
            Some(sp(
                loc,
                EL::Unpack(en, tys_opt, E::FieldBindings::Named(efields)),
            ))
        }
        PE::Call(pn, None, ptys_opt, sp!(_, exprs)) => {
            let pkg = context.current_package();
            context
                .env()
                .check_feature(pkg, FeatureGate::PositionalFields, loc);
            let en = context.name_access_chain_to_module_access(Access::ApplyNamed, pn)?;
            let tys_opt = optional_types(context, ptys_opt);
            let pfields: Option<_> = exprs.into_iter().map(|e| assign(context, e)).collect();
            Some(sp(
                loc,
                EL::Unpack(en, tys_opt, E::FieldBindings::Positional(pfields?)),
            ))
        }
        _ => {
            context.env().add_diag(diag!(
                Syntax::InvalidLValue,
                (
                    loc,
                    "Invalid assignment syntax. Expected: a local, a field write, or a \
                     deconstructing assignment"
                )
            ));
            None
        }
    }
}

fn assign_unpack_fields(
    context: &mut Context,
    loc: Loc,
    pfields: Vec<(Field, P::Exp)>,
) -> Option<Fields<E::LValue>> {
    let afields = pfields
        .into_iter()
        .map(|(f, e)| Some((f, assign(context, e)?)))
        .collect::<Option<_>>()?;
    Some(named_fields(
        context,
        loc,
        "deconstructing assignment",
        "assignment binding",
        afields,
    ))
}

fn mutability(context: &mut Context, _loc: Loc, pmut: P::Mutability) -> E::Mutability {
    let pkg = context.current_package();
    let supports_let_mut = context.env().supports_feature(pkg, FeatureGate::LetMut);
    match pmut {
        Some(loc) => {
            assert!(supports_let_mut, "ICE mut should not parse without let mut");
            E::Mutability::Mut(loc)
        }
        None if supports_let_mut => E::Mutability::Imm,
        // Mark as imm to force errors during migration
        None if context.env().edition(pkg) == Edition::E2024_MIGRATION => E::Mutability::Imm,
        // without let mut enabled, all locals are mutable and do not need the annotation
        None => E::Mutability::Either,
    }
}

//**************************************************************************************************
// Valid names
//**************************************************************************************************

fn check_valid_address_name(
    context: &mut DefnContext,
    sp!(_, ln_): &P::LeadingNameAccess,
) -> Result<(), ()> {
    use P::LeadingNameAccess_ as LN;
    match ln_ {
        LN::AnonymousAddress(_) => Ok(()),
        LN::GlobalAddress(n) | LN::Name(n) => {
            check_restricted_name_all_cases(context, NameCase::Address, n)
        }
    }
}

fn check_valid_function_parameter_name(context: &mut Context, is_macro: Option<Loc>, v: &Var) {
    const SYNTAX_IDENTIFIER_NOTE: &str =
        "'macro' parameters start with '$' to indicate that their arguments are not evaluated \
        before the macro is expanded, meaning the entire expression is substituted. \
        This is different from regular function parameters that are evaluated before the \
        function is called.";
    let is_syntax_identifier = v.is_syntax_identifier();
    if let Some(macro_loc) = is_macro {
        if !is_syntax_identifier && !v.is_underscore() {
            let msg = format!(
                "Invalid parameter name '{}'. '{}' parameter names must start with '$' (or must be '_')",
                v, MACRO_MODIFIER,
            );
            let macro_msg = format!("Declared '{}' here", MACRO_MODIFIER);
            let mut diag = diag!(
                Declarations::InvalidName,
                (v.loc(), msg),
                (macro_loc, macro_msg),
            );
            diag.add_note(SYNTAX_IDENTIFIER_NOTE);
            context.env().add_diag(diag);
        }
    } else if is_syntax_identifier {
        let msg = format!(
            "Invalid parameter name '{}'. Non-'{}' parameter names cannot start with '$'",
            v, MACRO_MODIFIER,
        );
        let mut diag = diag!(Declarations::InvalidName, (v.loc(), msg));
        diag.add_note(SYNTAX_IDENTIFIER_NOTE);
        context.env().add_diag(diag);
    } else if !is_valid_local_variable_name(v.value()) {
        let msg = format!(
            "Invalid parameter name '{}'. Local variable names must start with 'a'..'z', '_', \
            or be a valid name quoted with backticks (`name`)",
            v,
        );
        context
            .env()
            .add_diag(diag!(Declarations::InvalidName, (v.loc(), msg)));
    }
    let _ = check_restricted_name_all_cases(&mut context.defn_context, NameCase::Variable, &v.0);
}

fn check_valid_local_name(context: &mut Context, v: &Var) {
    if !is_valid_local_variable_name(v.value()) {
        let msg = format!(
            "Invalid local name '{}'. Local variable names must start with 'a'..'z', '_', \
            or be a valid name quoted with backticks (`name`)",
            v,
        );
        context
            .env()
            .add_diag(diag!(Declarations::InvalidName, (v.loc(), msg)));
    }
    let _ = check_restricted_name_all_cases(&mut context.defn_context, NameCase::Variable, &v.0);
}

fn is_valid_local_variable_name(s: Symbol) -> bool {
    Var::is_valid_name(s) && !Var::is_syntax_identifier_name(s)
}

#[derive(Copy, Clone, Debug)]
pub enum ModuleMemberKind {
    Constant,
    Function,
    Struct,
}

impl ModuleMemberKind {
    pub fn case(self) -> NameCase {
        match self {
            ModuleMemberKind::Constant => NameCase::Constant,
            ModuleMemberKind::Function => NameCase::Function,
            ModuleMemberKind::Struct => NameCase::Struct,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum NameCase {
    Constant,
    Function,
    Struct,
    Module,
    ModuleMemberAlias(ModuleMemberKind),
    ModuleAlias,
    Variable,
    Address,
    TypeParameter,
}

impl NameCase {
    pub const fn name(&self) -> &'static str {
        match self {
            NameCase::Constant => "constant",
            NameCase::Function => "function",
            NameCase::Struct => "struct",
            NameCase::Module => "module",
            NameCase::ModuleMemberAlias(ModuleMemberKind::Function) => "function alias",
            NameCase::ModuleMemberAlias(ModuleMemberKind::Constant) => "constant alias",
            NameCase::ModuleMemberAlias(ModuleMemberKind::Struct) => "struct alias",
            NameCase::ModuleAlias => "module alias",
            NameCase::Variable => "variable",
            NameCase::Address => "address",
            NameCase::TypeParameter => "type parameter",
        }
    }
}

fn check_valid_module_member_name(
    context: &mut Context,
    member: ModuleMemberKind,
    name: Name,
) -> Option<Name> {
    match check_valid_module_member_name_impl(context, member, &name, member.case()) {
        Err(()) => None,
        Ok(()) => Some(name),
    }
}

fn check_valid_module_member_alias(
    context: &mut Context,
    member: ModuleMemberKind,
    alias: Name,
) -> Option<Name> {
    match check_valid_module_member_name_impl(
        context,
        member,
        &alias,
        NameCase::ModuleMemberAlias(member),
    ) {
        Err(()) => None,
        Ok(()) => Some(alias),
    }
}

fn check_valid_module_member_name_impl(
    context: &mut Context,
    member: ModuleMemberKind,
    n: &Name,
    case: NameCase,
) -> Result<(), ()> {
    use ModuleMemberKind as M;
    fn upper_first_letter(s: &str) -> String {
        let mut chars = s.chars();
        match chars.next() {
            None => String::new(),
            Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
        }
    }
    match member {
        M::Function => {
            if n.value.starts_with(|c| c == '_') {
                let msg = format!(
                    "Invalid {} name '{}'. {} names cannot start with '_'",
                    case.name(),
                    n,
                    upper_first_letter(case.name()),
                );
                context
                    .env()
                    .add_diag(diag!(Declarations::InvalidName, (n.loc, msg)));
                return Err(());
            }
        }
        M::Constant | M::Struct => {
            if !is_valid_struct_or_constant_name(&n.value) {
                let msg = format!(
                    "Invalid {} name '{}'. {} names must start with 'A'..'Z'",
                    case.name(),
                    n,
                    upper_first_letter(case.name()),
                );
                context
                    .env()
                    .add_diag(diag!(Declarations::InvalidName, (n.loc, msg)));
                return Err(());
            }
        }
    }

    // TODO move these names to a more central place?
    check_restricted_names(
        context,
        case,
        n,
        crate::naming::ast::BuiltinFunction_::all_names(),
    )?;
    check_restricted_names(
        context,
        case,
        n,
        crate::naming::ast::BuiltinTypeName_::all_names(),
    )?;

    // Restricting Self for now in the case where we ever have impls
    // Otherwise, we could allow it
    check_restricted_name_all_cases(&mut context.defn_context, case, n)?;

    Ok(())
}

fn check_valid_type_parameter_name(
    context: &mut Context,
    is_macro: Option<Loc>,
    n: &Name,
) -> Result<(), ()> {
    const SYNTAX_IDENTIFIER_NOTE: &str = "Type parameter names starting with '$' indicate that \
        their arguments do not have to satisfy certain constraints before the macro is expanded, \
        meaning types like '&mut u64' or '(bool, u8)' may be used as arguments.";

    let is_syntax_ident = Var::is_syntax_identifier_name(n.value);
    if let Some(macro_loc) = is_macro {
        if !is_syntax_ident {
            let msg = format!(
                "Invalid type parameter name. \
                '{} fun' type parameter names must start with '$'",
                MACRO_MODIFIER
            );
            let macro_msg = format!("Declared '{}' here", MACRO_MODIFIER);
            let mut diag = diag!(
                Declarations::InvalidName,
                (n.loc, msg),
                (macro_loc, macro_msg),
            );
            diag.add_note(SYNTAX_IDENTIFIER_NOTE);
            context.env().add_diag(diag);
        }
    } else if is_syntax_ident {
        let msg = format!(
            "Invalid type parameter name. \
                Only '{} fun' type parameter names cat start with '$'",
            MACRO_MODIFIER
        );
        let mut diag = diag!(Declarations::InvalidName, (n.loc, msg));
        diag.add_note(SYNTAX_IDENTIFIER_NOTE);
        context.env().add_diag(diag);
    }

    // TODO move these names to a more central place?
    if n.value == symbol!("_") {
        let diag = restricted_name_error(NameCase::TypeParameter, n.loc, "_");
        context.env().add_diag(diag);
        return Err(());
    }
    check_restricted_names(
        context,
        NameCase::TypeParameter,
        n,
        crate::naming::ast::BuiltinFunction_::all_names(),
    )?;
    check_restricted_names(
        context,
        NameCase::TypeParameter,
        n,
        crate::naming::ast::BuiltinTypeName_::all_names(),
    )?;

    check_restricted_name_all_cases(&mut context.defn_context, NameCase::TypeParameter, n)
}

pub fn is_valid_struct_or_constant_name(s: &str) -> bool {
    s.starts_with(|c: char| c.is_ascii_uppercase())
}

// Checks for a restricted name in any decl case
// Self and vector are not allowed
fn check_restricted_name_all_cases(
    context: &mut DefnContext,
    case: NameCase,
    n: &Name,
) -> Result<(), ()> {
    match case {
        NameCase::Constant
        | NameCase::Function
        | NameCase::Struct
        | NameCase::Module
        | NameCase::ModuleMemberAlias(_)
        | NameCase::ModuleAlias
        | NameCase::Address => {
            if Var::is_syntax_identifier_name(n.value) {
                let msg = format!(
                    "Invalid {} name '{}'. Identifiers starting with '$' can be used only for \
                    parameters and type paramters",
                    case.name(),
                    n,
                );
                context
                    .env
                    .add_diag(diag!(Declarations::InvalidName, (n.loc, msg)));
                return Err(());
            }
        }
        NameCase::Variable | NameCase::TypeParameter => (),
    }

    let n_str = n.value.as_str();
    let can_be_vector = matches!(case, NameCase::Module | NameCase::ModuleAlias);
    if n_str == ModuleName::SELF_NAME
        || (!can_be_vector && n_str == crate::naming::ast::BuiltinTypeName_::VECTOR)
    {
        context
            .env
            .add_diag(restricted_name_error(case, n.loc, n_str));
        Err(())
    } else {
        Ok(())
    }
}

fn check_restricted_names(
    context: &mut Context,
    case: NameCase,
    sp!(loc, n_): &Name,
    all_names: &BTreeSet<Symbol>,
) -> Result<(), ()> {
    if all_names.contains(n_) {
        context
            .env()
            .add_diag(restricted_name_error(case, *loc, n_));
        Err(())
    } else {
        Ok(())
    }
}

fn restricted_name_error(case: NameCase, loc: Loc, restricted: &str) -> Diagnostic {
    let a_or_an = match case.name().chars().next().unwrap() {
        // TODO this is not exhaustive to the indefinite article rules in English
        // but 'case' is never user generated, so it should be okay for a while/forever...
        'a' | 'e' | 'i' | 'o' | 'u' => "an",
        _ => "a",
    };
    let msg = format!(
        "Invalid {case} name '{restricted}'. '{restricted}' is restricted and cannot be used to \
         name {a_or_an} {case}",
        a_or_an = a_or_an,
        case = case.name(),
        restricted = restricted,
    );
    diag!(NameResolution::ReservedName, (loc, msg))
}
