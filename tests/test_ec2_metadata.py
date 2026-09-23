# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for connector-subnet auto-selection (read-only EC2, fake client).

Drives ``select_connector_subnets`` with a fake EC2 client returning scripted
DescribeSubnets / DescribeRouteTables responses. No AWS.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.ec2_metadata import (
    Ec2MetadataError,
    select_connector_subnets,
)


class _FakeEc2:
    def __init__(self, subnets, route_tables, *, raise_on=None):
        self._subnets = subnets
        self._rts = route_tables
        self._raise_on = raise_on or set()

    def describe_subnets(self, **kw):
        if "describe_subnets" in self._raise_on:
            raise RuntimeError("denied")
        return {"Subnets": self._subnets}

    def describe_route_tables(self, **kw):
        if "describe_route_tables" in self._raise_on:
            raise RuntimeError("denied")
        return {"RouteTables": self._rts}


def _subnet(sid, az):
    return {"SubnetId": sid, "AvailabilityZone": az}


def _nat_route():
    return {"DestinationCidrBlock": "0.0.0.0/0", "NatGatewayId": "nat-1"}


def _igw_route():
    return {"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": "igw-1"}


def test_happy_two_nat_subnets_two_az() -> None:
    subnets = [_subnet("subnet-a", "us-east-1a"), _subnet("subnet-b", "us-east-1b")]
    rts = [
        {
            "Associations": [{"SubnetId": "subnet-a"}, {"SubnetId": "subnet-b"}],
            "Routes": [_nat_route()],
        }
    ]
    sel = select_connector_subnets(_FakeEc2(subnets, rts), "vpc-1")
    assert sel.can_auto_select is True
    assert sel.subnet_ids == "subnet-a,subnet-b"
    assert sel.az_count == 2


def test_only_igw_cannot_auto_select() -> None:
    subnets = [_subnet("subnet-a", "us-east-1a"), _subnet("subnet-b", "us-east-1b")]
    rts = [{"Associations": [{"Main": True}], "Routes": [_igw_route()]}]
    sel = select_connector_subnets(_FakeEc2(subnets, rts), "vpc-1")
    assert sel.can_auto_select is False
    assert sel.subnet_ids is None
    assert "NAT" in sel.reason


def test_single_az_nat_cannot_auto_select() -> None:
    subnets = [_subnet("subnet-a", "us-east-1a"), _subnet("subnet-a2", "us-east-1a")]
    rts = [
        {
            "Associations": [{"SubnetId": "subnet-a"}, {"SubnetId": "subnet-a2"}],
            "Routes": [_nat_route()],
        }
    ]
    sel = select_connector_subnets(_FakeEc2(subnets, rts), "vpc-1")
    assert sel.can_auto_select is False
    assert sel.az_count == 1
    assert "1 availability zone" in sel.reason


def test_unassociated_subnets_use_main_route_table() -> None:
    # Subnets with no explicit RT association fall back to the VPC main RT (NAT).
    subnets = [_subnet("subnet-a", "us-east-1a"), _subnet("subnet-b", "us-east-1b")]
    rts = [{"Associations": [{"Main": True}], "Routes": [_nat_route()]}]
    sel = select_connector_subnets(_FakeEc2(subnets, rts), "vpc-1")
    assert sel.can_auto_select is True
    assert sel.subnet_ids == "subnet-a,subnet-b"


def test_explicit_association_overrides_main() -> None:
    # subnet-a explicitly on an IGW RT, subnet-b on main NAT RT → only 1 AZ NAT.
    subnets = [_subnet("subnet-a", "us-east-1a"), _subnet("subnet-b", "us-east-1b")]
    rts = [
        {"Associations": [{"SubnetId": "subnet-a"}], "Routes": [_igw_route()]},
        {"Associations": [{"Main": True}], "Routes": [_nat_route()]},
    ]
    sel = select_connector_subnets(_FakeEc2(subnets, rts), "vpc-1")
    # subnet-a is IGW (explicit), subnet-b is NAT (main) → only 1 NAT AZ.
    assert sel.can_auto_select is False
    assert sel.az_count == 1


def test_no_subnets_in_vpc() -> None:
    sel = select_connector_subnets(_FakeEc2([], []), "vpc-x")
    assert sel.can_auto_select is False
    assert "No subnets" in sel.reason
    assert "same region" in sel.reason


def test_empty_vpc_id() -> None:
    sel = select_connector_subnets(_FakeEc2([], []), "")
    assert sel.can_auto_select is False
    assert sel.subnet_ids is None


def test_api_error_raises() -> None:
    client = _FakeEc2([], [], raise_on={"describe_subnets"})
    with pytest.raises(Ec2MetadataError):
        select_connector_subnets(client, "vpc-1")


def test_three_az_picks_one_per_az() -> None:
    subnets = [
        _subnet("subnet-a", "us-east-1a"),
        _subnet("subnet-a2", "us-east-1a"),
        _subnet("subnet-b", "us-east-1b"),
        _subnet("subnet-c", "us-east-1c"),
    ]
    rts = [
        {
            "Associations": [{"SubnetId": s} for s in
                             ("subnet-a", "subnet-a2", "subnet-b", "subnet-c")],
            "Routes": [_nat_route()],
        }
    ]
    sel = select_connector_subnets(_FakeEc2(subnets, rts), "vpc-1")
    assert sel.can_auto_select is True
    assert sel.az_count == 3
    # One per AZ (the first by sort order within each AZ).
    assert sel.subnet_ids == "subnet-a,subnet-b,subnet-c"


def test_excluded_az_dropped_still_two_az() -> None:
    # Three NAT AZs; excluding the MSK-unsupported one leaves two → still selects.
    subnets = [
        _subnet("subnet-a", "us-east-1a"),
        _subnet("subnet-b", "us-east-1b"),
        _subnet("subnet-d", "us-east-1d"),
    ]
    rts = [
        {
            "Associations": [{"SubnetId": s} for s in
                             ("subnet-a", "subnet-b", "subnet-d")],
            "Routes": [_nat_route()],
        }
    ]
    sel = select_connector_subnets(
        _FakeEc2(subnets, rts), "vpc-1", excluded_azs={"us-east-1d"}
    )
    assert sel.can_auto_select is True
    assert sel.subnet_ids == "subnet-a,subnet-b"
    assert sel.az_count == 2
    assert "us-east-1d" in sel.reason


def test_excluded_az_leaves_too_few_azs() -> None:
    # Two NAT AZs; excluding one leaves a single AZ → cannot auto-select, and the
    # reason names the excluded AZ so the retry's give-up is explained.
    subnets = [_subnet("subnet-a", "us-east-1a"), _subnet("subnet-d", "us-east-1d")]
    rts = [
        {
            "Associations": [{"SubnetId": "subnet-a"}, {"SubnetId": "subnet-d"}],
            "Routes": [_nat_route()],
        }
    ]
    sel = select_connector_subnets(
        _FakeEc2(subnets, rts), "vpc-1", excluded_azs={"us-east-1d"}
    )
    assert sel.can_auto_select is False
    assert sel.subnet_ids is None
    assert "us-east-1d" in sel.reason


# ---------------------------------------------------------------------------
# diagnose_cdc_network — discovered / create / blocked
# ---------------------------------------------------------------------------

from dsql_migrator.core.ec2_metadata import diagnose_cdc_network  # noqa: E402


class _FakeEc2Vpc(_FakeEc2):
    """_FakeEc2 + describe_vpcs (for the CIDR-carving in diagnose_cdc_network)."""

    def __init__(self, subnets, route_tables, vpcs, *, raise_on=None):
        super().__init__(subnets, route_tables, raise_on=raise_on)
        self._vpcs = vpcs

    def describe_vpcs(self, **kw):
        if "describe_vpcs" in self._raise_on:
            raise RuntimeError("denied")
        return {"Vpcs": self._vpcs}


def _csubnet(sid, az, cidr):
    return {"SubnetId": sid, "AvailabilityZone": az, "CidrBlock": cidr}


def _vpc(cidr):
    return {
        "VpcId": "vpc-1",
        "CidrBlock": cidr,
        "CidrBlockAssociationSet": [
            {"CidrBlock": cidr, "CidrBlockState": {"State": "associated"}}
        ],
    }


def test_diagnose_discovered_reuses_existing_nat_subnets() -> None:
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"SubnetId": "subnet-a"}, {"SubnetId": "subnet-b"}],
            "Routes": [_nat_route()]}]
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "discovered"
    assert d.connector_subnet_ids == "subnet-a,subnet-b"


def test_diagnose_create_when_igw_and_free_cidrs() -> None:
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": [_igw_route()]}]
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "create"
    assert d.nat_public_subnet_id == "subnet-a"
    assert d.private_subnet_cidrs == ["10.0.2.0/24", "10.0.3.0/24"]
    assert d.availability_zones == ["us-east-1a", "us-east-1b"]


def test_diagnose_blocked_no_public_subnet() -> None:
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": []}]  # no egress at all
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "blocked"
    assert "public" in d.reason


def test_diagnose_blocked_single_az() -> None:
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": [_igw_route()]}]
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "blocked"
    assert "AZ" in d.reason


def test_diagnose_blocked_no_free_cidr() -> None:
    # A /23 VPC splits into exactly two /24s, both already used → no free space.
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": [_igw_route()]}]
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/23")]), "vpc-1")
    assert d.mode == "blocked"
    assert "free /24" in d.reason


def test_diagnose_create_uses_secondary_cidr_when_primary_full() -> None:
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": [_igw_route()]}]
    vpc = {
        "VpcId": "vpc-1",
        "CidrBlock": "10.0.0.0/23",  # primary fully used
        "CidrBlockAssociationSet": [
            {"CidrBlock": "10.0.0.0/23", "CidrBlockState": {"State": "associated"}},
            {"CidrBlock": "10.1.0.0/16", "CidrBlockState": {"State": "associated"}},
        ],
    }
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [vpc]), "vpc-1")
    assert d.mode == "create"
    assert d.private_subnet_cidrs[0].startswith("10.1.")


def test_diagnose_nonexistent_vpc_says_not_found() -> None:
    # No subnets AND describe_vpcs returns no VPC -> the typical wrong-VpcId typo.
    # The message must point at the VPC ID, not misdirect toward "wrong region".
    d = diagnose_cdc_network(_FakeEc2Vpc([], [], []), "vpc-typo")
    assert d.mode == "blocked"
    assert "was not found" in d.reason
    assert "check the VPC ID" in d.reason
    assert "vpc-typo" in d.reason


def test_diagnose_real_vpc_with_no_subnets_says_add_subnets() -> None:
    # No subnets BUT the VPC exists -> a real-but-empty VPC; distinct guidance.
    d = diagnose_cdc_network(_FakeEc2Vpc([], [], [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "blocked"
    assert "exists but has no subnets" in d.reason
    assert "not found" not in d.reason


def test_vpc_exists_treats_api_uncertainty_as_exists() -> None:
    # A permissions/throttle error on describe_vpcs must NOT masquerade as "not found"
    # (that would send a user with a correct VpcId to fix a non-problem). Uncertain =>
    # assume it exists, so the diagnosis falls through to the real-VPC message.
    from dsql_migrator.core.ec2_metadata import _vpc_exists

    client = _FakeEc2Vpc([], [], [], raise_on={"describe_vpcs"})  # raises RuntimeError
    assert _vpc_exists(client, "vpc-1") is True


def test_diagnose_api_error_raises() -> None:
    client = _FakeEc2Vpc([], [], [], raise_on={"describe_subnets"})
    with pytest.raises(Ec2MetadataError):
        diagnose_cdc_network(client, "vpc-1")


# ---------------------------------------------------------------------------
# Off-VPC routing awareness (TGW / peering / VPN) for "create" mode
# ---------------------------------------------------------------------------

from dsql_migrator.core.ec2_metadata import _extract_routed_cidrs  # noqa: E402


def test_extract_routed_cidrs_picks_tgw_peering_vgw_only() -> None:
    rts = [{"Routes": [
        {"DestinationCidrBlock": "0.0.0.0/0", "GatewayId": "igw-1"},   # egress, skip
        {"DestinationCidrBlock": "10.0.0.0/16", "GatewayId": "local"},  # local, skip
        {"DestinationCidrBlock": "10.99.0.0/16", "VpcPeeringConnectionId": "pcx-1"},
        {"DestinationCidrBlock": "172.16.0.0/12", "TransitGatewayId": "tgw-1"},
        {"DestinationCidrBlock": "192.168.0.0/16", "GatewayId": "vgw-1"},
    ]}]
    assert _extract_routed_cidrs(rts) == [
        "10.99.0.0/16", "172.16.0.0/12", "192.168.0.0/16"
    ]


def test_extract_routed_cidrs_empty_when_no_off_vpc_routes() -> None:
    rts = [{"Routes": [
        {"DestinationCidrBlock": "0.0.0.0/0", "NatGatewayId": "nat-1"},
        {"DestinationCidrBlock": "10.0.0.0/16", "GatewayId": "local"},
    ]}]
    assert _extract_routed_cidrs(rts) == []


def test_diagnose_create_warns_when_vpc_routes_off_vpc() -> None:
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": [
        _igw_route(),
        {"DestinationCidrBlock": "192.168.0.0/16", "GatewayId": "vgw-1"},
    ]}]
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "create"
    assert d.routed_cidr_warning is not None
    assert "VPN" in d.routed_cidr_warning or "peering" in d.routed_cidr_warning
    assert "192.168.0.0/16" in d.routed_cidr_warning


def test_diagnose_create_no_warning_without_off_vpc_routes() -> None:
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": [_igw_route()]}]
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "create"
    assert d.routed_cidr_warning is None


def test_diagnose_create_avoids_carving_over_peered_range() -> None:
    # A peered range inside the VPC's own CIDR must be avoided by the carve.
    subnets = [_csubnet("subnet-a", "us-east-1a", "10.0.0.0/24"),
               _csubnet("subnet-b", "us-east-1b", "10.0.1.0/24")]
    rts = [{"Associations": [{"Main": True}], "Routes": [
        _igw_route(),
        # Route the next two /24s to a peering connection -> carve must skip them.
        {"DestinationCidrBlock": "10.0.2.0/24", "VpcPeeringConnectionId": "pcx-1"},
        {"DestinationCidrBlock": "10.0.3.0/24", "VpcPeeringConnectionId": "pcx-1"},
    ]}]
    d = diagnose_cdc_network(_FakeEc2Vpc(subnets, rts, [_vpc("10.0.0.0/16")]), "vpc-1")
    assert d.mode == "create"
    # The first two free /24s (.2 and .3) are routed to peering, so the carve
    # skips them and picks .4 / .5.
    assert d.private_subnet_cidrs == ["10.0.4.0/24", "10.0.5.0/24"]


# --- verify_subnet_egress tests ---------------------------------------------------

from dsql_migrator.core.ec2_metadata import verify_subnet_egress  # noqa: E402


class _FakeEc2Verify:
    """Minimal EC2 client for verify_subnet_egress tests."""

    def __init__(self, subnets, route_tables):
        self._subnets = subnets
        self._rts = route_tables

    def describe_subnets(self, **kw):
        ids = set(kw.get("SubnetIds", []))
        return {"Subnets": [s for s in self._subnets if s["SubnetId"] in ids]}

    def describe_route_tables(self, **kw):
        return {"RouteTables": self._rts}


def test_verify_subnet_egress_nat_passes() -> None:
    subnets = [
        {"SubnetId": "subnet-a", "VpcId": "vpc-1"},
        {"SubnetId": "subnet-b", "VpcId": "vpc-1"},
    ]
    rts = [{
        "VpcId": "vpc-1",
        "Associations": [{"SubnetId": "subnet-a"}, {"SubnetId": "subnet-b"}],
        "Routes": [_nat_route()],
    }]
    ok, reason = verify_subnet_egress(_FakeEc2Verify(subnets, rts), ["subnet-a", "subnet-b"])
    assert ok is True
    assert reason == ""


def test_verify_subnet_egress_igw_fails() -> None:
    subnets = [
        {"SubnetId": "subnet-a", "VpcId": "vpc-1"},
        {"SubnetId": "subnet-b", "VpcId": "vpc-1"},
    ]
    rts = [{
        "VpcId": "vpc-1",
        "Associations": [{"Main": True}],
        "Routes": [_igw_route()],
    }]
    ok, reason = verify_subnet_egress(_FakeEc2Verify(subnets, rts), ["subnet-a", "subnet-b"])
    assert ok is False
    assert "NAT gateway" in reason
    assert "subnet-a" in reason


def test_verify_subnet_egress_mixed_nat_and_igw_fails() -> None:
    subnets = [
        {"SubnetId": "subnet-nat", "VpcId": "vpc-1"},
        {"SubnetId": "subnet-igw", "VpcId": "vpc-1"},
    ]
    rts = [
        {
            "VpcId": "vpc-1",
            "Associations": [{"SubnetId": "subnet-nat"}],
            "Routes": [_nat_route()],
        },
        {
            "VpcId": "vpc-1",
            "Associations": [{"Main": True}],
            "Routes": [_igw_route()],
        },
    ]
    ok, reason = verify_subnet_egress(_FakeEc2Verify(subnets, rts), ["subnet-nat", "subnet-igw"])
    assert ok is False
    assert "subnet-igw" in reason
    assert "subnet-nat" not in reason


def test_verify_subnet_egress_empty_ids() -> None:
    ok, reason = verify_subnet_egress(_FakeEc2Verify([], []), [])
    assert ok is False
    assert "No subnet" in reason


# --- discover_host_network: "which network am I on, inside THIS VPC" -----------
# Fills the cdc-stack's HostSubnetCidr so ConnectorHostDiagnosticsIngress admits the
# app on MSK 9098 for the in-process (SeedMode=External) CDC seed a PostgreSQL source
# always uses.


class _FakeEc2Host:
    """Records the Filters it is handed, so the test can prove the VPC scoping."""

    def __init__(self, enis, vpcs, *, raise_on=None):
        self._enis = enis
        self._vpcs = vpcs
        self._raise_on = raise_on or set()
        self.eni_filters = None
        self.describe_eni_calls = 0

    def describe_network_interfaces(self, **kw):
        self.describe_eni_calls += 1
        self.eni_filters = kw.get("Filters")
        if "describe_network_interfaces" in self._raise_on:
            raise RuntimeError("AccessDenied")
        return {"NetworkInterfaces": self._enis}

    def describe_vpcs(self, **kw):
        if "describe_vpcs" in self._raise_on:
            raise RuntimeError("AccessDenied")
        return {"Vpcs": self._vpcs}


def _vpc_two_blocks():
    # A secondary-CIDR VPC: the ENCLOSING block must be picked, not the first one.
    return [
        {
            "CidrBlock": "10.0.0.0/16",
            "CidrBlockAssociationSet": [
                {"CidrBlock": "10.0.0.0/16", "CidrBlockState": {"State": "associated"}},
                {
                    "CidrBlock": "100.64.0.0/16",
                    "CidrBlockState": {"State": "associated"},
                },
            ],
        }
    ]


def test_discover_host_network_filters_by_this_address_and_the_entered_vpc() -> None:
    from dsql_migrator.core.ec2_metadata import HostNetwork, discover_host_network

    fake = _FakeEc2Host(
        [{"VpcId": "vpc-app", "SubnetId": "subnet-b"}], _vpc_two_blocks()
    )
    got = discover_host_network(fake, "vpc-app", ip="100.64.3.9")

    # The VPC block CONTAINING the address -- picking blocks[0] would yield
    # 10.0.0.0/16, an MSK ingress rule matching nobody.
    assert got == HostNetwork(
        ip="100.64.3.9",
        vpc_id="vpc-app",
        subnet_id="subnet-b",
        cidr="100.64.0.0/16",
    )
    # The lookup MUST be scoped to the VPC being deployed into. A private IPv4 address
    # is unique only within a VPC, so an unscoped lookup could match a foreign ENI and
    # register somebody else's CIDR as a real MSK ingress source. A fake client ignores
    # filters it is not asked about, so only this assertion can catch a dropped filter.
    assert fake.eni_filters == [
        {"Name": "addresses.private-ip-address", "Values": ["100.64.3.9"]},
        {"Name": "vpc-id", "Values": ["vpc-app"]},
    ]


@pytest.mark.parametrize(
    "enis,vpcs,raise_on,ip",
    [
        # Not in this VPC at all (the vpc-id filter matched nothing).
        ([], _vpc_two_blocks(), None, "10.0.1.5"),
        # Ambiguous: two ENIs carry the address -> refuse to guess.
        (
            [{"VpcId": "vpc-app", "SubnetId": "a"}, {"VpcId": "vpc-app", "SubnetId": "b"}],
            _vpc_two_blocks(),
            None,
            "10.0.1.5",
        ),
        # The describes are denied / throttled.
        (
            [{"VpcId": "vpc-app", "SubnetId": "a"}],
            _vpc_two_blocks(),
            {"describe_network_interfaces"},
            "10.0.1.5",
        ),
        (
            [{"VpcId": "vpc-app", "SubnetId": "a"}],
            _vpc_two_blocks(),
            {"describe_vpcs"},
            "10.0.1.5",
        ),
        # An address outside every associated block.
        ([{"VpcId": "vpc-app", "SubnetId": "a"}], _vpc_two_blocks(), None, "192.168.1.9"),
    ],
)
def test_discover_host_network_is_none_when_it_cannot_be_proven(
    enis, vpcs, raise_on, ip
) -> None:
    # "Unknown" is a valid answer and must NEVER raise: the caller turns it into a
    # blocker or a warning, and an exception here would break the deploy dialog.
    from dsql_migrator.core.ec2_metadata import discover_host_network

    fake = _FakeEc2Host(enis, vpcs, raise_on=raise_on)
    assert discover_host_network(fake, "vpc-app", ip=ip) is None


def test_discover_host_network_needs_a_vpc_and_an_address() -> None:
    from dsql_migrator.core import ec2_metadata as _em

    fake = _FakeEc2Host([{"VpcId": "vpc-app", "SubnetId": "a"}], _vpc_two_blocks())
    # No VpcId entered yet -> nothing to scope to, and no AWS call is made.
    assert _em.discover_host_network(fake, "", ip="10.0.1.5") is None
    assert fake.describe_eni_calls == 0
    # No resolvable local address (a sandbox with no route) -> same, still no call.
    fake2 = _FakeEc2Host([{"VpcId": "vpc-app", "SubnetId": "a"}], _vpc_two_blocks())
    import unittest.mock

    with unittest.mock.patch.object(_em, "local_ipv4", return_value=None):
        assert _em.discover_host_network(fake2, "vpc-app") is None
    assert fake2.describe_eni_calls == 0


def test_cidr_contains_is_total() -> None:
    from dsql_migrator.core.ec2_metadata import cidr_contains

    assert cidr_contains("10.0.0.0/16", "10.0.11.31") is True
    assert cidr_contains("172.31.0.0/20", "172.31.5.9") is True
    assert cidr_contains("172.31.0.0/20", "172.31.20.9") is False
    # Never raises on junk -- it gates a refusal, so a crash would be worse than False.
    for cidr, addr in (("", "10.0.0.1"), ("10.0.0.0/16", ""), ("nonsense", "x"), ("10.0.0.0/16", "::1")):
        assert cidr_contains(cidr, addr) is False


def test_local_ipv4_is_a_v4_address_or_none() -> None:
    # No AWS, no metadata endpoint: a UDP connect to a link-local address just binds
    # the socket, so this must return quickly and never raise anywhere it runs.
    import ipaddress

    from dsql_migrator.core.ec2_metadata import local_ipv4

    got = local_ipv4()
    assert got is None or isinstance(ipaddress.IPv4Address(got), ipaddress.IPv4Address)
