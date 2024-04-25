
# Astra
[![release version](https://img.shields.io/github/v/release/slackhq/astra?include_prereleases)](https://github.com/slackhq/astra/releases)
[![release pipeline](https://img.shields.io/github/actions/workflow/status/slackhq/astra/maven.yml?branch=master)](https://github.com/slackhq/astra/actions/workflows/maven.yml)
[![license](https://img.shields.io/github/license/slackhq/astra)](https://github.com/slackhq/astra/blob/master/LICENSE)
[![All Contributors](https://img.shields.io/github/all-contributors/slackhq/astra?color=ee8449)](#contributors)


Astra is a cloud-native search and analytics engine for log, trace, and audit data. It is designed to be easy to operate, 
cost-effective, and scale to petabytes of data.

https://slackhq.github.io/astra/

## Goals
- Native support for log, trace, audit use cases.
- Aggressively prioritize ingest of recent data over older data.
- Full-text search capability.
- First-class Kubernetes support for all components.
- Autoscaling of ingest and query capacity.
- Coordination free ingestion, so failure of a single node does not impact ingestion.
- Works out of the box with sensible defaults.
- Designed for zero data loss.
- First-class Grafana support with [accompanying plugin](https://github.com/slackhq/slack-astra-app).
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

## Licensing
Licensed under [MIT](LICENSE). Copyright (c) 2024 Slack.

## Contributors

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/vthacker"><img src="https://avatars.githubusercontent.com/u/158041?v=4?s=100" width="100px;" alt="Varun Thacker"/><br /><sub><b>Varun Thacker</b></sub></a><br /><a href="#code-vthacker" title="Code">💻</a> <a href="#doc-vthacker" title="Documentation">📖</a> <a href="#review-vthacker" title="Reviewed Pull Requests">👀</a> <a href="#bug-vthacker" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/bryanlb"><img src="https://avatars.githubusercontent.com/u/771133?v=4?s=100" width="100px;" alt="Bryan Burkholder"/><br /><sub><b>Bryan Burkholder</b></sub></a><br /><a href="#code-bryanlb" title="Code">💻</a> <a href="#doc-bryanlb" title="Documentation">📖</a> <a href="#review-bryanlb" title="Reviewed Pull Requests">👀</a> <a href="#bug-bryanlb" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kyle-sammons"><img src="https://avatars.githubusercontent.com/u/1023070?v=4?s=100" width="100px;" alt="Kyle Sammons"/><br /><sub><b>Kyle Sammons</b></sub></a><br /><a href="#plugin-kyle-sammons" title="Plugin/utility libraries">🔌</a> <a href="#code-kyle-sammons" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://www.linkedin.com/in/mansu"><img src="https://avatars.githubusercontent.com/u/93836?v=4?s=100" width="100px;" alt="Suman Karumuri"/><br /><sub><b>Suman Karumuri</b></sub></a><br /><a href="#code-mansu" title="Code">💻</a> <a href="#review-mansu" title="Reviewed Pull Requests">👀</a> <a href="#ideas-mansu" title="Ideas, Planning, & Feedback">🤔</a> <a href="#talk-mansu" title="Talks">📢</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
