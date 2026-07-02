"""Auto-select MSK Connect subnets for a VPC (read-only EC2 discovery).

The CDC connectors run as MSK Connect tasks on ENIs that have NO public IP, so
they reach Aurora DSQL (public endpoint only), Secrets Manager, STS, Glue, and
CloudWatch through a NAT gateway. This module inspects a VPC's subnets + route
tables and picks the subnets that have ``0.0.0.0/0 -> nat-…`` egress, one per
Availability Zone (MSK Serverless wants >=2 AZs). This lets the deploy form ask
the customer for only a VpcId instead of a list of subnet ids.

Everything is read-only (``ec2:DescribeSubnets`` + ``ec2:DescribeRouteTables``)
and shares the single profile-aware boto3 session, mirroring
:mod:`dsql_migrator.core.dsql_metadata`. The boto3 client is injectable for
tests via the same ``BotoSessionLike`` seam used elsewhere.

Limitation: egress via a Transit Gateway (``tgw-…``) or VPC peering (``pcx-…``)
is NOT recognized as NAT egress, so such VPCs fall back to manual subnet entry
(the :attr:`SubnetSelection.reason` explains this).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dsql_migrator.core.aws_session import BotoSessionLike, build_session


class Ec2MetadataError(RuntimeError):
    """An EC2 describe call failed during subnet discovery."""


@dataclass(frozen=True)
class SubnetInfo:
    """One subnet's id, AZ, and classified internet-egress path."""

    subnet_id: str
    az: str
    egress_type: str  # "nat" | "igw" | "none"


@dataclass(frozen=True)
class SubnetSelection:
    """The result of auto-selecting connector subnets for a VPC.

    ``subnet_ids`` is a comma-separated string of the chosen subnets (one per AZ,
    NAT-egress) when :attr:`can_auto_select` is True, else ``None``. ``subnets``
    lists every NAT-egress subnet found (for display). ``reason`` always carries a
    human-readable explanation (success summary or why auto-select was not
    possible) so the UI can show it verbatim.
    """

    subnet_ids: Optional[str]
    subnets: list[SubnetInfo] = field(default_factory=list)
    az_count: int = 0
    can_auto_select: bool = False
    reason: str = ""


def build_ec2_client(
    aws_profile: Optional[str], region: Optional[str]
) -> BotoSessionLike:
    """Build an EC2 client from the shared session (honoring the global profile)."""
    return build_session(aws_profile).client("ec2", region_name=region)


def _classify_egress(routes: list[dict]) -> str:
    """Classify a route table's default-route egress as nat / igw / none.

    Looks for the ``0.0.0.0/0`` route and returns ``"nat"`` when it targets a NAT
    gateway, ``"igw"`` for an internet gateway, else ``"none"`` (no default route,
    or egress via an unrecognized target such as a transit gateway / peering).
    """
    for route in routes:
        if route.get("DestinationCidrBlock") != "0.0.0.0/0":
            continue
        nat = route.get("NatGatewayId")
        if nat:
            return "nat"
        gw = route.get("GatewayId") or ""
        if gw.startswith("nat-"):
            return "nat"
        if gw.startswith("igw-"):
            return "igw"
        return "none"
    return "none"


def select_connector_subnets(
    ec2_client: BotoSessionLike, vpc_id: str
) -> SubnetSelection:
    """Auto-select NAT-egress connector subnets for ``vpc_id`` (one per AZ, >=2).

    Reads the VPC's subnets and route tables, classifies each subnet's egress via
    its associated route table (explicit subnet association first, otherwise the
    VPC's main route table), keeps the NAT-egress subnets, and picks one per
    distinct AZ. Auto-selects only when >=2 AZs are covered; otherwise returns
    ``can_auto_select=False`` with a reason so the caller can ask for manual
    subnet ids. Read-only; raises :class:`Ec2MetadataError` only on an API error.
    """
    vpc_id = (vpc_id or "").strip()
    if not vpc_id:
        return SubnetSelection(subnet_ids=None, reason="No VPC id provided.")
    try:
        subnets_resp = ec2_client.describe_subnets(  # type: ignore[attr-defined]
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        rt_resp = ec2_client.describe_route_tables(  # type: ignore[attr-defined]
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
    except Exception as exc:  # noqa: BLE001 - surface as a typed error
        raise Ec2MetadataError(
            f"Could not read subnets/route tables for VPC '{vpc_id}': "
            f"{str(exc).splitlines()[0]}"
        ) from exc

    subnets = subnets_resp.get("Subnets", []) or []
    if not subnets:
        return SubnetSelection(
            subnet_ids=None,
            reason=(
                f"No subnets found in VPC '{vpc_id}'. Ensure the VPC is in the "
                "same region as your Aurora DSQL cluster."
            ),
        )

    # Map each subnet to its route table's egress: explicit association wins, else
    # the VPC main route table covers any subnet without an explicit association.
    route_tables = rt_resp.get("RouteTables", []) or []
    explicit: dict[str, str] = {}  # subnet_id -> egress_type
    main_egress = "none"
    for rt in route_tables:
        egress = _classify_egress(rt.get("Routes", []) or [])
        is_main = False
        for assoc in rt.get("Associations", []) or []:
            if assoc.get("Main"):
                is_main = True
            sid = assoc.get("SubnetId")
            if sid:
                explicit[sid] = egress
        if is_main:
            main_egress = egress

    classified: list[SubnetInfo] = []
    for s in subnets:
        sid = s.get("SubnetId")
        az = s.get("AvailabilityZone", "")
        if not sid:
            continue
        egress = explicit.get(sid, main_egress)
        classified.append(SubnetInfo(subnet_id=sid, az=az, egress_type=egress))

    nat_subnets = [s for s in classified if s.egress_type == "nat"]
    # One NAT subnet per distinct AZ (sorted for deterministic selection).
    by_az: dict[str, SubnetInfo] = {}
    for s in sorted(nat_subnets, key=lambda x: (x.az, x.subnet_id)):
        by_az.setdefault(s.az, s)
    chosen = list(by_az.values())
    az_count = len(by_az)

    if az_count >= 2:
        ids = ",".join(s.subnet_id for s in chosen)
        return SubnetSelection(
            subnet_ids=ids,
            subnets=nat_subnets,
            az_count=az_count,
            can_auto_select=True,
            reason=(
                f"Auto-selected {len(chosen)} NAT-egress subnets across "
                f"{az_count} availability zones."
            ),
        )

    if az_count == 1:
        reason = (
            "Found NAT-egress subnets in only 1 availability zone; MSK Connect "
            "needs >=2. Add a NAT-routed subnet in another AZ, or enter subnet "
            "ids manually."
        )
    else:
        reason = (
            f"No NAT-gateway-routed subnets found in VPC '{vpc_id}'. The "
            "connectors need NAT egress to reach DSQL, Secrets Manager, STS, "
            "Glue and CloudWatch. Add a NAT gateway (in >=2 AZs), or — if you use "
            "a Transit Gateway or VPC peering for egress — enter subnet ids "
            "manually."
        )
    return SubnetSelection(
        subnet_ids=None,
        subnets=nat_subnets,
        az_count=az_count,
        can_auto_select=False,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# Unified VPC egress diagnosis (VpcId-only deploy: discover / create / blocked)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CdcNetworkDiagnosis:
    """Outcome of diagnosing a VPC's egress for the CDC connectors.

    ``mode`` is the recommended action:
      * ``"discovered"`` -- the VPC already has NAT-egress private subnets in >=2
        AZs; ``connector_subnet_ids`` is the comma-separated set to reuse as-is
        (no new resources).
      * ``"create"`` -- no NAT-egress subnets, but an IGW-routed public subnet and
        free /24 space exist, so the cdc-stack can create its OWN private subnets +
        NAT in this VPC. ``nat_public_subnet_id`` / ``private_subnet_cidrs`` /
        ``availability_zones`` carry the computed inputs.
      * ``"blocked"`` -- neither path is possible; ``reason`` explains why and the
        UI should offer the manual subnet override instead.
    """

    mode: str  # "discovered" | "create" | "blocked"
    reason: str
    connector_subnet_ids: Optional[str] = None
    nat_public_subnet_id: Optional[str] = None
    nat_public_subnet_az: Optional[str] = None
    private_subnet_cidrs: list[str] = field(default_factory=list)
    availability_zones: list[str] = field(default_factory=list)
    # In "create" mode, a non-fatal caution when the VPC forwards traffic off-VPC
    # via a transit gateway / peering / VPN (routes the tool can see) -- because
    # ranges it CANNOT see (e.g. a summarized on-prem supernet) might overlap the
    # auto-carved /24s. Empty when no such routing exists. The UI shows it so the
    # operator can choose the manual subnet override for a complex VPC.
    routed_cidr_warning: Optional[str] = None


def _extract_routed_cidrs(route_tables: list[dict]) -> list[str]:
    """Return non-local destination CIDRs routed off-VPC via TGW / peering / VGW.

    These are ranges the VPC forwards somewhere the tool cannot enumerate (a
    transit gateway's other attachments, a peered VPC, or on-premises networks
    reached over a VPN/Direct-Connect virtual gateway). A connector subnet carved
    to overlap one of them would create a routing ambiguity, so the caller both
    (a) avoids overlapping these *known* ranges when carving and (b) warns that
    *unknown* ranges (e.g. a summarized on-prem supernet) might still collide.

    Skips the default route (``0.0.0.0/0`` -- that is the egress path, handled by
    :func:`_classify_egress`) and local routes (NAT/IGW/local, which stay in-VPC).
    """
    routed: list[str] = []
    for rt in route_tables:
        for route in rt.get("Routes", []) or []:
            dest = route.get("DestinationCidrBlock")
            if not dest or dest == "0.0.0.0/0":
                continue
            gw = route.get("GatewayId") or ""
            off_vpc = (
                route.get("TransitGatewayId")
                or route.get("VpcPeeringConnectionId")
                or gw.startswith("vgw-")
                or gw.startswith("tgw-")
            )
            if off_vpc and dest not in routed:
                routed.append(dest)
    return routed


def _classify_vpc_subnets(
    ec2_client: BotoSessionLike, vpc_id: str
) -> tuple[list[SubnetInfo], dict[str, str], list[str]]:
    """Return ``(classified_subnets, subnet_cidrs, routed_cidrs)`` for a VPC.

    Shared by :func:`diagnose_cdc_network`: reads subnets + route tables once,
    classifies each subnet's egress (explicit association first, else the VPC main
    route table). ``subnet_cidrs`` maps subnet id -> CIDR block (for free-range
    carving). ``routed_cidrs`` are off-VPC destinations (TGW/peering/VGW) the carve
    must avoid + warn about (see :func:`_extract_routed_cidrs`). Raises
    :class:`Ec2MetadataError` on an API error; returns ``([], {}, [])`` for an
    empty VPC (the caller decides how to report it).
    """
    try:
        subnets_resp = ec2_client.describe_subnets(  # type: ignore[attr-defined]
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        rt_resp = ec2_client.describe_route_tables(  # type: ignore[attr-defined]
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
    except Exception as exc:  # noqa: BLE001 - surface as a typed error
        raise Ec2MetadataError(
            f"Could not read subnets/route tables for VPC '{vpc_id}': "
            f"{str(exc).splitlines()[0]}"
        ) from exc

    subnets = subnets_resp.get("Subnets", []) or []
    route_tables = rt_resp.get("RouteTables", []) or []
    explicit: dict[str, str] = {}
    main_egress = "none"
    for rt in route_tables:
        egress = _classify_egress(rt.get("Routes", []) or [])
        is_main = False
        for assoc in rt.get("Associations", []) or []:
            if assoc.get("Main"):
                is_main = True
            sid = assoc.get("SubnetId")
            if sid:
                explicit[sid] = egress
        if is_main:
            main_egress = egress

    classified: list[SubnetInfo] = []
    cidrs: dict[str, str] = {}
    for s in subnets:
        sid = s.get("SubnetId")
        az = s.get("AvailabilityZone", "")
        if not sid:
            continue
        classified.append(
            SubnetInfo(subnet_id=sid, az=az, egress_type=explicit.get(sid, main_egress))
        )
        cidr = s.get("CidrBlock")
        if cidr:
            cidrs[sid] = cidr
    return classified, cidrs, _extract_routed_cidrs(route_tables)


def _vpc_cidr_blocks(ec2_client: BotoSessionLike, vpc_id: str) -> list[str]:
    """Return a VPC's associated IPv4 CIDR blocks (primary + secondary)."""
    try:
        resp = ec2_client.describe_vpcs(VpcIds=[vpc_id])  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001
        raise Ec2MetadataError(
            f"Could not read VPC '{vpc_id}': {str(exc).splitlines()[0]}"
        ) from exc
    vpcs = resp.get("Vpcs", []) or []
    if not vpcs:
        return []
    vpc = vpcs[0]
    blocks: list[str] = []
    primary = vpc.get("CidrBlock")
    if primary:
        blocks.append(primary)
    for assoc in vpc.get("CidrBlockAssociationSet", []) or []:
        state = (assoc.get("CidrBlockState") or {}).get("State")
        cidr = assoc.get("CidrBlock")
        if cidr and cidr not in blocks and (state is None or state == "associated"):
            blocks.append(cidr)
    return blocks


def _find_free_subnet_cidrs(
    vpc_cidrs: list[str], used_cidrs: list[str], *, count: int = 2, new_prefix: int = 24
) -> list[str]:
    """Find ``count`` free /``new_prefix`` blocks within the VPC CIDRs.

    Iterates each VPC CIDR's /new_prefix subnets in order, skipping any that
    overlap an existing (used) subnet CIDR, and returns the first ``count`` free
    ranges as strings. Returns fewer than ``count`` (possibly empty) when space is
    exhausted -- the caller treats that as "blocked". Pure (stdlib ipaddress).
    """
    import ipaddress

    used_nets = []
    for c in used_cidrs:
        try:
            used_nets.append(ipaddress.ip_network(c, strict=False))
        except ValueError:
            continue
    free: list[str] = []
    for vpc_cidr in vpc_cidrs:
        try:
            vpc_net = ipaddress.ip_network(vpc_cidr, strict=False)
        except ValueError:
            continue
        if vpc_net.version != 4 or vpc_net.prefixlen > new_prefix:
            continue  # cannot carve a /new_prefix out of this block
        for candidate in vpc_net.subnets(new_prefix=new_prefix):
            if any(candidate.overlaps(u) for u in used_nets):
                continue
            free.append(str(candidate))
            if len(free) >= count:
                return free
    return free


def diagnose_cdc_network(
    ec2_client: BotoSessionLike, vpc_id: str
) -> CdcNetworkDiagnosis:
    """Diagnose a VPC's egress and recommend discover / create / blocked.

    The "VpcId only" deploy path: given just a VPC, decide whether the connectors
    can use existing NAT-egress subnets (discovered), whether the cdc-stack should
    create its own private subnets + NAT (create), or whether neither is possible
    (blocked -> the UI offers the manual subnet override). Read-only; raises
    :class:`Ec2MetadataError` only on an API error.
    """
    vpc_id = (vpc_id or "").strip()
    if not vpc_id:
        return CdcNetworkDiagnosis(mode="blocked", reason="No VPC id provided.")

    classified, cidrs, routed_cidrs = _classify_vpc_subnets(ec2_client, vpc_id)
    if not classified:
        return CdcNetworkDiagnosis(
            mode="blocked",
            reason=(
                f"No subnets found in VPC '{vpc_id}'. Ensure the VPC is in the "
                "same region as your Aurora DSQL cluster."
            ),
        )

    # --- A. discovered: existing NAT-egress subnets in >=2 AZs -----------------
    nat_by_az: dict[str, SubnetInfo] = {}
    for s in sorted(
        (x for x in classified if x.egress_type == "nat"),
        key=lambda x: (x.az, x.subnet_id),
    ):
        nat_by_az.setdefault(s.az, s)
    if len(nat_by_az) >= 2:
        ids = ",".join(s.subnet_id for s in nat_by_az.values())
        return CdcNetworkDiagnosis(
            mode="discovered",
            connector_subnet_ids=ids,
            availability_zones=list(nat_by_az.keys()),
            reason=(
                f"Found existing NAT-egress subnets in {len(nat_by_az)} AZs; "
                "the connectors will use them (no new resources)."
            ),
        )

    # --- B. create: an IGW public subnet + free /24 space + >=2 AZs ------------
    igw_subnets = sorted(
        (x for x in classified if x.egress_type == "igw"),
        key=lambda x: (x.az, x.subnet_id),
    )
    all_azs = sorted({x.az for x in classified if x.az})
    if not igw_subnets:
        return CdcNetworkDiagnosis(
            mode="blocked",
            reason=(
                f"VPC '{vpc_id}' has no NAT-egress subnets and no public "
                "(internet-gateway-routed) subnet to place a NAT gateway in. Add a "
                "public subnet with an internet gateway, or enter connector subnet "
                "ids manually (e.g. if you use a Transit Gateway / VPC peering for "
                "egress)."
            ),
        )
    if len(all_azs) < 2:
        return CdcNetworkDiagnosis(
            mode="blocked",
            reason=(
                f"VPC '{vpc_id}' has subnets in only {len(all_azs)} AZ; MSK "
                "Serverless needs >=2. Add a subnet in a second AZ, or enter "
                "connector subnet ids manually."
            ),
        )
    vpc_cidrs = _vpc_cidr_blocks(ec2_client, vpc_id)
    # Avoid carving over existing subnets AND over ranges the VPC routes off-VPC
    # (TGW/peering/VPN) -- those would create a routing ambiguity for the new subnet.
    free = _find_free_subnet_cidrs(
        vpc_cidrs, list(cidrs.values()) + routed_cidrs, count=2
    )
    if len(free) < 2:
        return CdcNetworkDiagnosis(
            mode="blocked",
            reason=(
                f"VPC '{vpc_id}' has no two free /24 CIDR blocks to create private "
                "connector subnets in (the address space looks fully allocated, or "
                "the free ranges overlap routes to a transit gateway / peering / "
                "VPN). Free up space, or enter connector subnet ids manually."
            ),
        )
    nat_subnet = igw_subnets[0]
    azs = all_azs[:2]
    # Non-fatal caution: the tool can avoid the off-VPC ranges it can SEE, but a
    # summarized on-prem supernet advertised over VPN/DX may not appear as an
    # explicit route. For a VPC with such connectivity, recommend the override.
    routed_warning = None
    if routed_cidrs:
        routed_warning = (
            "This VPC routes traffic off-VPC (transit gateway / VPC peering / VPN: "
            f"{', '.join(routed_cidrs[:4])}{'…' if len(routed_cidrs) > 4 else ''}). "
            "The auto-carved subnets avoid those ranges, but if your network "
            "advertises a broader on-premises range that is not an explicit route "
            "here, the new /24s could overlap it. If unsure, use the Advanced "
            "subnet override with subnets your network team confirms are free."
        )
    return CdcNetworkDiagnosis(
        mode="create",
        connector_subnet_ids=None,
        nat_public_subnet_id=nat_subnet.subnet_id,
        nat_public_subnet_az=nat_subnet.az,
        private_subnet_cidrs=free[:2],
        availability_zones=azs,
        routed_cidr_warning=routed_warning,
        reason=(
            f"No existing NAT egress; the stack will create a NAT gateway in "
            f"{nat_subnet.az} ({nat_subnet.subnet_id}) and two private subnets "
            f"{free[0]} ({azs[0]}) / {free[1]} ({azs[1]}). A NAT gateway bills "
            "hourly and is removed when you delete the CDC infrastructure."
        ),
    )


__all__ = [
    "Ec2MetadataError",
    "SubnetInfo",
    "SubnetSelection",
    "CdcNetworkDiagnosis",
    "build_ec2_client",
    "select_connector_subnets",
    "diagnose_cdc_network",
]
