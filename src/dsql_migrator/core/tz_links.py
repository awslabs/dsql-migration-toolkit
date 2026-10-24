# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# GENERATED from the tzdata.zi of tzdata 2026c-rearguard (its 257 "L <zone> <link>" lines).
# Do not edit TZDATA_VERSION or TZ_LINKS by hand. To take another tzdata release, run from
# the repo root:
#   .venv/bin/python -m dsql_migrator.core.tz_links /usr/share/zoneinfo/tzdata.zi

"""tzdata's links (G192): each name tzdata gives a zone besides the zone's own, and that zone.

A link is the zone it names, under another name. tzdata keeps old names that way
(``US/Eastern``, ``Asia/Calcutta``, ``Japan``, ``ROK``), the names of the zones it merged
because they agree since 1970 (``Europe/Amsterdam`` is ``Europe/Brussels``), and since 2024b
the abbreviation-named zones (``CET`` is ``Europe/Brussels`` too). Which names a PostgreSQL
build knows is up to the tzdata it was built with: Debian ships the old names apart
(tzdata-legacy), so the postgres 16 and 17 images know no ``US/Eastern``, and there
``AT TIME ZONE 'US/Eastern'`` passes CREATE TABLE and CREATE VIEW and fails (22023) only
when a row takes the default or the view is read. :mod:`dsql_migrator.core.source_time_zone`
writes a source zone named by a link as the zone it links to, a name every build has.

``TZ_LINKS`` maps each link's name in lower case -- MySQL and PostgreSQL read a zone name
without regard to case -- to its zone as tzdata spells it. The command above rewrites it
and ``TZDATA_VERSION`` from a tzdata.zi, the zic input each tzdata release ships, built
without tzdata's backzone data -- macOS's /usr/share/zoneinfo/tzdata.zi, or ``make
tzdata.zi`` in tzdata's source -- so that each link names a zone every build has. Debian's
is built with it (its ``# ddeps backzone`` line) -- many links are zones of their own there,
and a few point at those -- so the command refuses it. Pure data otherwise.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

TZDATA_VERSION = "2026c-rearguard"

TZ_LINKS: dict[str, str] = {
    "africa/accra": "Africa/Abidjan",
    "africa/addis_ababa": "Africa/Nairobi",
    "africa/asmara": "Africa/Nairobi",
    "africa/asmera": "Africa/Nairobi",
    "africa/bamako": "Africa/Abidjan",
    "africa/bangui": "Africa/Lagos",
    "africa/banjul": "Africa/Abidjan",
    "africa/blantyre": "Africa/Maputo",
    "africa/brazzaville": "Africa/Lagos",
    "africa/bujumbura": "Africa/Maputo",
    "africa/conakry": "Africa/Abidjan",
    "africa/dakar": "Africa/Abidjan",
    "africa/dar_es_salaam": "Africa/Nairobi",
    "africa/djibouti": "Africa/Nairobi",
    "africa/douala": "Africa/Lagos",
    "africa/freetown": "Africa/Abidjan",
    "africa/gaborone": "Africa/Maputo",
    "africa/harare": "Africa/Maputo",
    "africa/kampala": "Africa/Nairobi",
    "africa/kigali": "Africa/Maputo",
    "africa/kinshasa": "Africa/Lagos",
    "africa/libreville": "Africa/Lagos",
    "africa/lome": "Africa/Abidjan",
    "africa/luanda": "Africa/Lagos",
    "africa/lubumbashi": "Africa/Maputo",
    "africa/lusaka": "Africa/Maputo",
    "africa/malabo": "Africa/Lagos",
    "africa/maseru": "Africa/Johannesburg",
    "africa/mbabane": "Africa/Johannesburg",
    "africa/mogadishu": "Africa/Nairobi",
    "africa/niamey": "Africa/Lagos",
    "africa/nouakchott": "Africa/Abidjan",
    "africa/ouagadougou": "Africa/Abidjan",
    "africa/porto-novo": "Africa/Lagos",
    "africa/timbuktu": "Africa/Abidjan",
    "america/anguilla": "America/Puerto_Rico",
    "america/antigua": "America/Puerto_Rico",
    "america/argentina/comodrivadavia": "America/Argentina/Catamarca",
    "america/aruba": "America/Puerto_Rico",
    "america/atikokan": "America/Panama",
    "america/atka": "America/Adak",
    "america/blanc-sablon": "America/Puerto_Rico",
    "america/buenos_aires": "America/Argentina/Buenos_Aires",
    "america/catamarca": "America/Argentina/Catamarca",
    "america/cayman": "America/Panama",
    "america/coral_harbour": "America/Panama",
    "america/cordoba": "America/Argentina/Cordoba",
    "america/creston": "America/Phoenix",
    "america/curacao": "America/Puerto_Rico",
    "america/dominica": "America/Puerto_Rico",
    "america/ensenada": "America/Tijuana",
    "america/fort_wayne": "America/Indiana/Indianapolis",
    "america/godthab": "America/Nuuk",
    "america/grenada": "America/Puerto_Rico",
    "america/guadeloupe": "America/Puerto_Rico",
    "america/indianapolis": "America/Indiana/Indianapolis",
    "america/jujuy": "America/Argentina/Jujuy",
    "america/knox_in": "America/Indiana/Knox",
    "america/kralendijk": "America/Puerto_Rico",
    "america/louisville": "America/Kentucky/Louisville",
    "america/lower_princes": "America/Puerto_Rico",
    "america/marigot": "America/Puerto_Rico",
    "america/mendoza": "America/Argentina/Mendoza",
    "america/montreal": "America/Toronto",
    "america/montserrat": "America/Puerto_Rico",
    "america/nassau": "America/Toronto",
    "america/nipigon": "America/Toronto",
    "america/pangnirtung": "America/Iqaluit",
    "america/port_of_spain": "America/Puerto_Rico",
    "america/porto_acre": "America/Rio_Branco",
    "america/rainy_river": "America/Winnipeg",
    "america/rosario": "America/Argentina/Cordoba",
    "america/santa_isabel": "America/Tijuana",
    "america/shiprock": "America/Denver",
    "america/st_barthelemy": "America/Puerto_Rico",
    "america/st_kitts": "America/Puerto_Rico",
    "america/st_lucia": "America/Puerto_Rico",
    "america/st_thomas": "America/Puerto_Rico",
    "america/st_vincent": "America/Puerto_Rico",
    "america/thunder_bay": "America/Toronto",
    "america/tortola": "America/Puerto_Rico",
    "america/virgin": "America/Puerto_Rico",
    "america/yellowknife": "America/Edmonton",
    "antarctica/dumontdurville": "Pacific/Port_Moresby",
    "antarctica/mcmurdo": "Pacific/Auckland",
    "antarctica/south_pole": "Pacific/Auckland",
    "antarctica/syowa": "Asia/Riyadh",
    "arctic/longyearbyen": "Europe/Berlin",
    "asia/aden": "Asia/Riyadh",
    "asia/ashkhabad": "Asia/Ashgabat",
    "asia/bahrain": "Asia/Qatar",
    "asia/brunei": "Asia/Kuching",
    "asia/calcutta": "Asia/Kolkata",
    "asia/choibalsan": "Asia/Ulaanbaatar",
    "asia/chongqing": "Asia/Shanghai",
    "asia/chungking": "Asia/Shanghai",
    "asia/dacca": "Asia/Dhaka",
    "asia/harbin": "Asia/Shanghai",
    "asia/istanbul": "Europe/Istanbul",
    "asia/kashgar": "Asia/Urumqi",
    "asia/katmandu": "Asia/Kathmandu",
    "asia/kuala_lumpur": "Asia/Singapore",
    "asia/kuwait": "Asia/Riyadh",
    "asia/macao": "Asia/Macau",
    "asia/muscat": "Asia/Dubai",
    "asia/phnom_penh": "Asia/Bangkok",
    "asia/rangoon": "Asia/Yangon",
    "asia/saigon": "Asia/Ho_Chi_Minh",
    "asia/tel_aviv": "Asia/Jerusalem",
    "asia/thimbu": "Asia/Thimphu",
    "asia/ujung_pandang": "Asia/Makassar",
    "asia/ulan_bator": "Asia/Ulaanbaatar",
    "asia/vientiane": "Asia/Bangkok",
    "atlantic/faeroe": "Atlantic/Faroe",
    "atlantic/jan_mayen": "Europe/Berlin",
    "atlantic/reykjavik": "Africa/Abidjan",
    "atlantic/st_helena": "Africa/Abidjan",
    "australia/act": "Australia/Sydney",
    "australia/canberra": "Australia/Sydney",
    "australia/currie": "Australia/Hobart",
    "australia/lhi": "Australia/Lord_Howe",
    "australia/north": "Australia/Darwin",
    "australia/nsw": "Australia/Sydney",
    "australia/queensland": "Australia/Brisbane",
    "australia/south": "Australia/Adelaide",
    "australia/tasmania": "Australia/Hobart",
    "australia/victoria": "Australia/Melbourne",
    "australia/west": "Australia/Perth",
    "australia/yancowinna": "Australia/Broken_Hill",
    "brazil/acre": "America/Rio_Branco",
    "brazil/denoronha": "America/Noronha",
    "brazil/east": "America/Sao_Paulo",
    "brazil/west": "America/Manaus",
    "canada/atlantic": "America/Halifax",
    "canada/central": "America/Winnipeg",
    "canada/eastern": "America/Toronto",
    "canada/mountain": "America/Edmonton",
    "canada/newfoundland": "America/St_Johns",
    "canada/pacific": "America/Vancouver",
    "canada/saskatchewan": "America/Regina",
    "canada/yukon": "America/Whitehorse",
    "cet": "Europe/Brussels",
    "chile/continental": "America/Santiago",
    "chile/easterisland": "Pacific/Easter",
    "cst6cdt": "America/Chicago",
    "cuba": "America/Havana",
    "eet": "Europe/Athens",
    "egypt": "Africa/Cairo",
    "eire": "Europe/Dublin",
    "est": "America/Panama",
    "est5edt": "America/New_York",
    "etc/gmt+0": "Etc/GMT",
    "etc/gmt-0": "Etc/GMT",
    "etc/gmt0": "Etc/GMT",
    "etc/greenwich": "Etc/GMT",
    "etc/uct": "Etc/UTC",
    "etc/universal": "Etc/UTC",
    "etc/zulu": "Etc/UTC",
    "europe/amsterdam": "Europe/Brussels",
    "europe/belfast": "Europe/London",
    "europe/bratislava": "Europe/Prague",
    "europe/busingen": "Europe/Zurich",
    "europe/copenhagen": "Europe/Berlin",
    "europe/guernsey": "Europe/London",
    "europe/isle_of_man": "Europe/London",
    "europe/jersey": "Europe/London",
    "europe/kiev": "Europe/Kyiv",
    "europe/ljubljana": "Europe/Belgrade",
    "europe/luxembourg": "Europe/Brussels",
    "europe/mariehamn": "Europe/Helsinki",
    "europe/monaco": "Europe/Paris",
    "europe/nicosia": "Asia/Nicosia",
    "europe/oslo": "Europe/Berlin",
    "europe/podgorica": "Europe/Belgrade",
    "europe/san_marino": "Europe/Rome",
    "europe/sarajevo": "Europe/Belgrade",
    "europe/skopje": "Europe/Belgrade",
    "europe/stockholm": "Europe/Berlin",
    "europe/tiraspol": "Europe/Chisinau",
    "europe/uzhgorod": "Europe/Kyiv",
    "europe/vaduz": "Europe/Zurich",
    "europe/vatican": "Europe/Rome",
    "europe/zagreb": "Europe/Belgrade",
    "europe/zaporozhye": "Europe/Kyiv",
    "gb": "Europe/London",
    "gb-eire": "Europe/London",
    "gmt": "Etc/GMT",
    "gmt+0": "Etc/GMT",
    "gmt-0": "Etc/GMT",
    "gmt0": "Etc/GMT",
    "greenwich": "Etc/GMT",
    "hongkong": "Asia/Hong_Kong",
    "hst": "Pacific/Honolulu",
    "iceland": "Africa/Abidjan",
    "indian/antananarivo": "Africa/Nairobi",
    "indian/christmas": "Asia/Bangkok",
    "indian/cocos": "Asia/Yangon",
    "indian/comoro": "Africa/Nairobi",
    "indian/kerguelen": "Indian/Maldives",
    "indian/mahe": "Asia/Dubai",
    "indian/mayotte": "Africa/Nairobi",
    "indian/reunion": "Asia/Dubai",
    "iran": "Asia/Tehran",
    "israel": "Asia/Jerusalem",
    "jamaica": "America/Jamaica",
    "japan": "Asia/Tokyo",
    "kwajalein": "Pacific/Kwajalein",
    "libya": "Africa/Tripoli",
    "met": "Europe/Brussels",
    "mexico/bajanorte": "America/Tijuana",
    "mexico/bajasur": "America/Mazatlan",
    "mexico/general": "America/Mexico_City",
    "mst": "America/Phoenix",
    "mst7mdt": "America/Denver",
    "navajo": "America/Denver",
    "nz": "Pacific/Auckland",
    "nz-chat": "Pacific/Chatham",
    "pacific/chuuk": "Pacific/Port_Moresby",
    "pacific/enderbury": "Pacific/Kanton",
    "pacific/funafuti": "Pacific/Tarawa",
    "pacific/johnston": "Pacific/Honolulu",
    "pacific/majuro": "Pacific/Tarawa",
    "pacific/midway": "Pacific/Pago_Pago",
    "pacific/pohnpei": "Pacific/Guadalcanal",
    "pacific/ponape": "Pacific/Guadalcanal",
    "pacific/saipan": "Pacific/Guam",
    "pacific/samoa": "Pacific/Pago_Pago",
    "pacific/truk": "Pacific/Port_Moresby",
    "pacific/wake": "Pacific/Tarawa",
    "pacific/wallis": "Pacific/Tarawa",
    "pacific/yap": "Pacific/Port_Moresby",
    "poland": "Europe/Warsaw",
    "portugal": "Europe/Lisbon",
    "prc": "Asia/Shanghai",
    "pst8pdt": "America/Los_Angeles",
    "roc": "Asia/Taipei",
    "rok": "Asia/Seoul",
    "singapore": "Asia/Singapore",
    "turkey": "Europe/Istanbul",
    "uct": "Etc/UTC",
    "universal": "Etc/UTC",
    "us/alaska": "America/Anchorage",
    "us/aleutian": "America/Adak",
    "us/arizona": "America/Phoenix",
    "us/central": "America/Chicago",
    "us/east-indiana": "America/Indiana/Indianapolis",
    "us/eastern": "America/New_York",
    "us/hawaii": "Pacific/Honolulu",
    "us/indiana-starke": "America/Indiana/Knox",
    "us/michigan": "America/Detroit",
    "us/mountain": "America/Denver",
    "us/pacific": "America/Los_Angeles",
    "us/samoa": "Pacific/Pago_Pago",
    "utc": "Etc/UTC",
    "w-su": "Europe/Moscow",
    "wet": "Europe/Lisbon",
    "zulu": "Etc/UTC",
}


def _regenerated(module: str, tzdata_zi: str) -> str:
    """``module`` -- this file's text -- with its header, ``TZDATA_VERSION`` and ``TZ_LINKS``
    taken from ``tzdata_zi``: its first line (``# version 2026c``) and its ``L <zone> <link>``
    lines, sorted by the link's name."""
    lines = tzdata_zi.splitlines()
    if not lines or not lines[0].startswith("# version "):
        raise ValueError("not a tzdata.zi: its first line is no '# version ...'")
    if any(line.startswith("# ddeps") and "backzone" in line.split() for line in lines[:5]):
        raise ValueError(
            "a tzdata.zi built with backzone data: take one built without it (macOS's, or "
            "'make tzdata.zi' in tzdata's source)"
        )
    version = lines[0].split()[2]
    links = sorted(
        (fields[2].lower(), fields[1])
        for fields in (line.split() for line in lines if line.startswith("L "))
    )
    table = "".join(f'    "{link}": "{zone}",\n' for link, zone in links)
    module = re.sub(
        r"tzdata\.zi of tzdata \S+ \(its \d+ ",
        f"tzdata.zi of tzdata {version} (its {len(links)} ",
        module,
        count=1,
    )
    module = re.sub(
        r'^TZDATA_VERSION = "[^"]*"$', f'TZDATA_VERSION = "{version}"', module, count=1, flags=re.M
    )
    return re.sub(
        r"^(TZ_LINKS: dict\[str, str\] = \{\n).*?^\}$",
        lambda match: match.group(1) + table + "}",
        module,
        count=1,
        flags=re.M | re.S,
    )


if __name__ == "__main__":
    _module = Path(__file__)
    _module.write_text(
        _regenerated(
            _module.read_text(encoding="utf-8"), Path(sys.argv[1]).read_text(encoding="utf-8")
        ),
        encoding="utf-8",
    )
