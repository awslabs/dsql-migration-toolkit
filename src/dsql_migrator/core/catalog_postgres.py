# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL source catalog facts the inventory does not carry: row-level security and
standalone sequences.

Neither is something the migration re-creates from the inventory -- Aurora DSQL has no
row-level security, and Schema Conversion creates no sequence that no column owns -- yet
both are exactly what the go/no-go report exists to surface: a tenant-isolation policy that
silently vanishes at cut over, or an invoice-number sequence the application keeps calling
``nextval()`` on. So they are read by a probe beside the inventory
(:meth:`~dsql_migrator.core.introspector.SourceIntrospector.source_catalog`) and turned
into assessment findings, rather than carried in the persisted (``extra="forbid"``)
inventory models, whose new fields would make a newer session snapshot unreadable by an
older build.

Pure data, standard library only, so the dialect that reads these facts, the assessor that
grades them and the prerequisite check that gates on them can all import this module
without an import cycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SourceLogin:
    """The role the tool reads the source as, and the two attributes that exempt a role
    from every row-level security policy (PostgreSQL ``pg_roles.rolsuper`` /
    ``rolbypassrls``). Neither is inherited through role membership."""

    name: str
    superuser: bool
    bypass_rls: bool


@dataclass(frozen=True)
class RowSecurityPolicy:
    """One ``pg_policies`` row: what the policy filters, for which command and roles."""

    name: str
    # ``ALL`` / ``SELECT`` / ``INSERT`` / ``UPDATE`` / ``DELETE``.
    command: str
    # False for an ``AS RESTRICTIVE`` policy.
    permissive: bool
    # ``public`` when the policy applies to every role.
    roles: tuple[str, ...] = ()
    # The USING expression (which existing rows are visible / affected), and the WITH CHECK
    # expression (which new rows may be written), as PostgreSQL renders them.
    using: Optional[str] = None
    with_check: Optional[str] = None


@dataclass(frozen=True)
class RowSecurityTable:
    """A source table with row-level security ENABLED, and its policies.

    ``table`` is named as the inventory names it (``schema.table``). ``login_owns`` is
    whether the source login has the owning role's privileges (``pg_has_role(owner,
    'USAGE')``: the owner itself or a member that inherits it), which is how PostgreSQL
    decides that the policies do not apply to it -- unless ``forced`` (``FORCE ROW LEVEL
    SECURITY``), which applies them to the owner too. ``policies`` is empty when row-level
    security is enabled with no policy: PostgreSQL then shows a role it applies to no rows
    at all (default deny).
    """

    table: str
    owner: str
    login_owns: bool
    forced: bool
    policies: tuple[RowSecurityPolicy, ...] = ()


@dataclass(frozen=True)
class RowSecurityFacts:
    """The tables with row-level security enabled, and the login reading them (``None``
    when the login's attributes could not be read)."""

    login: Optional[SourceLogin]
    tables: tuple[RowSecurityTable, ...] = ()


@dataclass(frozen=True)
class StandaloneSequence:
    """A source sequence no column owns (``pg_depend`` has neither an ``'a'`` link -- a
    ``serial`` column or ``OWNED BY`` -- nor an ``'i'`` link -- an identity column), with
    its ``pg_sequences`` definition.

    ``last_value`` is the last value written to disk, ``None`` when the sequence has never
    been used OR when the source user may not read it; ``readable`` tells the two apart
    (``has_sequence_privilege(..., 'SELECT,USAGE')``).
    """

    name: str
    data_type: str
    start_value: int
    min_value: int
    max_value: int
    increment: int
    cycle: bool
    cache_size: int
    last_value: Optional[int]
    readable: bool


@dataclass(frozen=True)
class PgCatalogFacts:
    """Everything the probe read. A part that could not be read is ``None``, and
    ``read_errors`` says which part and why (exception class only -- value-free), so the
    report can say what it did not check instead of reading "Ready" over it."""

    row_security: Optional[RowSecurityFacts] = None
    sequences: Optional[tuple[StandaloneSequence, ...]] = None
    read_errors: tuple[str, ...] = ()


def row_security_exemption(
    table: RowSecurityTable, login: Optional[SourceLogin]
) -> Optional[str]:
    """Why ``login`` reads every row of ``table`` despite its policies; ``""`` when it does
    not (the policies filter what it reads); ``None`` when that cannot be told.

    PostgreSQL's own order (``check_enable_rls``): a superuser or a ``BYPASSRLS`` role is
    exempt from every policy; the table's owner is exempt unless the table is ``FORCE ROW
    LEVEL SECURITY``; every other role is subject to the policies.
    """
    if login is None:
        return None
    if login.superuser:
        return "a superuser"
    if login.bypass_rls:
        return "a role with BYPASSRLS"
    if table.login_owns and not table.forced:
        if login.name == table.owner:
            return "the table's owner"
        return f"a member of the table's owning role ({table.owner})"
    return ""


__all__ = [
    "PgCatalogFacts",
    "RowSecurityFacts",
    "RowSecurityPolicy",
    "RowSecurityTable",
    "SourceLogin",
    "StandaloneSequence",
    "row_security_exemption",
]
