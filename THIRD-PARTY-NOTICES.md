# Third-Party Notices

This project (`mysql-dsql-migrator`) is licensed under the Apache License 2.0
(see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE)).

It **bundles** the following pre-built third-party artifacts under
`connectors/plugins/` so the optional CDC pipeline can be deployed without a
local Java/Maven toolchain. Each component is the property of its respective
authors and is provided under its own license, listed below. This file is
informational; it does not modify any component's license.

## Runtime Python dependencies

Declared in `pyproject.toml` and installed from PyPI (not vendored):

| Package | License |
|---|---|
| boto3 | Apache-2.0 |
| nicegui | MIT |
| psycopg[binary] | LGPL-3.0 |
| pydantic | MIT |
| pymysql | MIT |
| sqlalchemy | MIT |
| sqlglot | MIT |

## Bundled Java / connector artifacts (`connectors/plugins/`)

| Component | Version | License | Upstream |
|---|---|---|---|
| Debezium (API, core, connector-binlog, connector-mysql, ddl-parser, storage-file, storage-kafka) | 2.7.4.Final | Apache-2.0 | https://github.com/debezium/debezium |
| ANTLR 4 Runtime | 4.10.1 | BSD-3-Clause | https://www.antlr.org/ |
| mysql-binlog-connector-java | 0.29.2 | Apache-2.0 | https://github.com/osheroff/mysql-binlog-connector-java |
| zstd-jni | 1.5.0-2 | BSD-2-Clause | https://github.com/luben/zstd-jni |
| msk-config-providers | 0.4.0 | Apache-2.0 | https://github.com/aws-samples/msk-config-providers |
| **MySQL Connector/J** | 8.3.0 | **GPL-2.0 w/ Universal FOSS Exception 1.0** | https://github.com/mysql/mysql-connector-j |

> **MySQL Connector/J licensing note.** MySQL Connector/J 8.3.0 Community is
> Copyright Oracle and/or its affiliates, licensed under **GPLv2 with the
> Universal FOSS Exception, version 1.0** — different terms than the Apache-2.0
> that governs the rest of this project.
>
> - **How it is used here:** bundled **unmodified** (the original
>   `mysql-connector-j-8.3.0.jar`, with its own `LICENSE` retained inside the jar)
>   solely as a runtime JDBC dependency of the Debezium MySQL connector for the
>   optional CDC data plane. This project does **not** link against, modify, or
>   incorporate its source.
> - **GPLv2 corresponding source:** because the jar is unmodified, the source
>   obligation is met by the upstream at
>   https://github.com/mysql/mysql-connector-j (tag `8.3.0`).
> - **Why the whole project is not GPL:** the Universal FOSS Exception 1.0
>   (http://oss.oracle.com/licenses/universal-foss-exception) permits combining
>   and distributing this Community connector with OSI-approved open-source
>   software (such as this Apache-2.0 project) **without** requiring the project
>   to be relicensed under the GPL.
> - **If unsuitable for you:** remove the bundled copy and have your build/deploy
>   step download it from the upstream source instead; the tool's Full Load path
>   does not require it.
>
> This summary is informational, not legal advice — have your organization's
> open-source/legal reviewers confirm before redistributing.

The Debezium distribution retains its upstream `LICENSE.txt` (Apache-2.0) inside
`connectors/plugins/debezium-connector-mysql/`.

## AWS sample code

`deploy/cdc-stack/lambda/cfnresponse.py` — Copyright Amazon.com, Inc. or its
affiliates; licensed under **MIT-0** (see the file header).
