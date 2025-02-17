
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
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/vthacker"><img src="https://avatars.githubusercontent.com/u/158041?v=4?s=100" width="100px;" alt="Varun Thacker"/><br /><sub><b>Varun Thacker</b></sub></a><br /><a href="#code-vthacker" title="Code">💻</a> <a href="#doc-vthacker" title="Documentation">📖</a> <a href="#review-vthacker" title="Reviewed Pull Requests">👀</a> <a href="#bug-vthacker" title="Bug reports">🐛</a> <a href="#ideas-vthacker" title="Ideas, Planning, & Feedback">🤔</a> <a href="#talk-vthacker" title="Talks">📢</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/bryanlb"><img src="https://avatars.githubusercontent.com/u/771133?v=4?s=100" width="100px;" alt="Bryan Burkholder"/><br /><sub><b>Bryan Burkholder</b></sub></a><br /><a href="#code-bryanlb" title="Code">💻</a> <a href="#doc-bryanlb" title="Documentation">📖</a> <a href="#review-bryanlb" title="Reviewed Pull Requests">👀</a> <a href="#bug-bryanlb" title="Bug reports">🐛</a> <a href="#ideas-bryanlb" title="Ideas, Planning, & Feedback">🤔</a> <a href="#talk-bryanlb" title="Talks">📢</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kyle-sammons"><img src="https://avatars.githubusercontent.com/u/1023070?v=4?s=100" width="100px;" alt="Kyle Sammons"/><br /><sub><b>Kyle Sammons</b></sub></a><br /><a href="#plugin-kyle-sammons" title="Plugin/utility libraries">🔌</a> <a href="#code-kyle-sammons" title="Code">💻</a> <a href="#bug-kyle-sammons" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://www.linkedin.com/in/mansu"><img src="https://avatars.githubusercontent.com/u/93836?v=4?s=100" width="100px;" alt="Suman Karumuri"/><br /><sub><b>Suman Karumuri</b></sub></a><br /><a href="#code-mansu" title="Code">💻</a> <a href="#review-mansu" title="Reviewed Pull Requests">👀</a> <a href="#ideas-mansu" title="Ideas, Planning, & Feedback">🤔</a> <a href="#talk-mansu" title="Talks">📢</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ermontross"><img src="https://avatars.githubusercontent.com/u/10778883?v=4?s=100" width="100px;" alt="Emma Montross"/><br /><sub><b>Emma Montross</b></sub></a><br /><a href="#plugin-ermontross" title="Plugin/utility libraries">🔌</a> <a href="#code-ermontross" title="Code">💻</a> <a href="#bug-ermontross" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/danhermann"><img src="https://avatars.githubusercontent.com/u/22777892?v=4?s=100" width="100px;" alt="Dan Hermann"/><br /><sub><b>Dan Hermann</b></sub></a><br /><a href="#code-danhermann" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/kai-xing-chen"><img src="https://avatars.githubusercontent.com/u/22359229?v=4?s=100" width="100px;" alt="Kai Chen"/><br /><sub><b>Kai Chen</b></sub></a><br /><a href="#code-kx-chen" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/autata"><img src="https://avatars.githubusercontent.com/u/24304518?v=4?s=100" width="100px;" alt="Aubrey"/><br /><sub><b>Aubrey</b></sub></a><br /><a href="#code-autata" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/shellywu815"><img src="https://avatars.githubusercontent.com/u/115680578?v=4?s=100" width="100px;" alt="Shelly Wu"/><br /><sub><b>Shelly Wu</b></sub></a><br /><a href="#code-shellywu815" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://www.solidspark.com"><img src="https://avatars.githubusercontent.com/u/1429729?v=4?s=100" width="100px;" alt="Ryan Katkov"/><br /><sub><b>Ryan Katkov</b></sub></a><br /><a href="#business-solidspark" title="Business development">💼</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://slack.com/"><img src="https://avatars.githubusercontent.com/u/6911160?v=4?s=100" width="100px;" alt="Slack"/><br /><sub><b>Slack</b></sub></a><br /><a href="#financial-slackhq" title="Financial">💵</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://opensource.salesforce.com"><img src="https://avatars.githubusercontent.com/u/453694?v=4?s=100" width="100px;" alt="Salesforce"/><br /><sub><b>Salesforce</b></sub></a><br /><a href="#financial-salesforce" title="Financial">💵</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/HenryCaiHaiying"><img src="https://avatars.githubusercontent.com/u/7378943?v=4?s=100" width="100px;" alt="Henry Haiying Cai"/><br /><sub><b>Henry Haiying Cai</b></sub></a><br /><a href="#code-HenryCaiHaiying" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/gjacoby126"><img src="https://avatars.githubusercontent.com/u/5717906?v=4?s=100" width="100px;" alt="Geoffrey Jacoby"/><br /><sub><b>Geoffrey Jacoby</b></sub></a><br /><a href="#bug-gjacoby126" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/zarna1parekh"><img src="https://avatars.githubusercontent.com/u/9705210?v=4?s=100" width="100px;" alt="Zarna Parekh"/><br /><sub><b>Zarna Parekh</b></sub></a><br /><a href="#code-zarna1parekh" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/baroquebobcat"><img src="https://avatars.githubusercontent.com/u/7378?v=4?s=100" width="100px;" alt="Nora Howard"/><br /><sub><b>Nora Howard</b></sub></a><br /><a href="#code-baroquebobcat" title="Code">💻</a> <a href="#bug-baroquebobcat" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/georgeluong"><img src="https://avatars.githubusercontent.com/u/4889254?v=4?s=100" width="100px;" alt="georgeluong"/><br /><sub><b>georgeluong</b></sub></a><br /><a href="#business-georgeluong" title="Business development">💼</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
