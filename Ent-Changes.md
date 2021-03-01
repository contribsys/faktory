# Faktory Enterprise Changelog

Changelog: [Faktory](https://github.com/contribsys/faktory/blob/master/Changes.md) || [Faktory Pro](https://github.com/contribsys/faktory/blob/master/Pro-Changes.md) || Faktory Enterprise

A trial version of Faktory Enterprise for OSX is available with each [release](/contribsys/faktory/releases/).
Click to purchase [Faktory Enterprise](https://billing.contribsys.com/fent/).

## 1.5.0

- Implement BYOR - **Bring Your Own Redis**. If Faktory Enterprise sees a
  `REDIS_URL` or `REDIS_PROVIDER` variable, it will use that to connect
  to Redis rather than starting its own Redis instance. This allows
  Faktory Enterprise to be used directly with AWS Elasticache, Heroku
  Redis and other SaaS providers. The Web UI /debug page will show you
  the current latency to Redis and warn if the latency is above 1ms.

## 1.4.2

- *No significant changes*

## 1.4.1

- Fix for `redis: transaction failed` error during batch processing under heavy load [#305]

## 1.4.0

- Major new feature: **[Job Tracking](https://github.com/contribsys/faktory/wiki/Ent-Tracking)** [#278]
- `-e staging` environment support, limited to 100 connections

## 1.3.0

- Allow custom WebUI tweaks for OEM whitelabeling by customers looking
  to integrate Faktory into their own product. [#270]
- Fix crash when pushing batch jobs [#274]

## 1.2.0

- Major features are [Batches](https://github.com/contribsys/faktory/wiki/Ent-Batches) and [Queue Throttling](https://github.com/contribsys/faktory/wiki/Ent-Throttling).
- Initial release.
