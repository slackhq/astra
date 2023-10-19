
# KalDB
[![release version](https://img.shields.io/github/v/release/slackhq/kaldb?include_prereleases)](https://github.com/slackhq/kaldb/releases)
[![release pipeline](https://img.shields.io/github/actions/workflow/status/slackhq/kaldb/maven.yml?branch=master)](https://github.com/slackhq/kaldb/actions/workflows/maven.yml)
[![license](https://img.shields.io/github/license/slackhq/kaldb)](https://github.com/slackhq/kaldb/blob/master/LICENSE)

KalDB is a cloud-native search and analytics engine for log, trace, and audit data. It is designed to be easy to operate, 
cost-effective, and scale to petabytes of data.

## Goals
- Native support for log, trace, audit use cases.
- Aggressively prioritize ingest of recent data over older data.
- Full-text search capability.
- First-class Kubernetes support for all components.
- Autoscaling of ingest and query capacity.
- Coordination free ingestion, so failure of a single node does not impact ingestion.
- Works out of the box with sensible defaults.
- Designed for zero data loss.
- First-class Grafana support with [accompanying plugin](https://github.com/slackhq/slack-kaldb-app).
- Built-in multi-tenancy, supporting several small use-cases on a single cluster.
- Supports the majority of Apache Lucene features.
- Drop-in replacement for most Opensearch log use cases.
- Operate with multiple cloud providers.

## Non-Goals
- General-purpose search cases, such as for an ecommerce site.
- Document mutability - records are expected to be append only.
- Additional storage engines other than Lucene.
- Support for JVM versions other than the current LTS.
- Supporting multiple Lucene versions.

## Wiki
Project roadmap, architecture diagrams, runbooks, and more are available on the [project wiki](https://github.com/slackhq/kaldb/wiki).

## Contributing
If you are interested in reporting/fixing issues and contributing directly to the code base, please see [CONTRIBUTING](.github/CONTRIBUTING.md) for more information on what we're looking for and how to get started.

## Licensing
Licensed under [MIT](LICENSE). Copyright (c) 2021 Slack.
