# Faktory Enterprise Changelog

Changelog: [Faktory](https://github.com/contribsys/faktory/blob/master/Changes.md) || Faktory Enterprise

A trial version of Faktory Enterprise for macOS is available with each [release](/contribsys/faktory/releases/).
Click to purchase [Faktory Enterprise](https://billing.contribsys.com/fent/).

## 1.8.0

- Migrate usage of SHA1 to SHA256 to appease linters
- Fix broken default Statsd namespacing [#433]

## 1.7.0

- Upgrade Redis driver to v9
- Upgrade Datadog driver to v5

## 1.6.2

- Fix crash when batch callback jobs retry [#408]

## 1.6.1

- Support `reserve_for` in cron jobs [#381]

## 1.6.0

- Add support for unlimited license without external network access for
  high security environments. See the Licensing wiki page for details.

## 1.5.5

- Cron jobs now support empty arguments and can configure expiry via `expires_in` [#384]
```toml
[[cron]]
  schedule = "*/5 * * * *"
  [cron.job]
    type = "FiveJob"
    args = []
    [cron.job.custom]
      expires_in = 60
```

## 1.5.4

- Upgrade to Go 1.17

## 1.5.1

- License check now supports HTTP(S)_PROXY env variables.
- Fix crash upon license check if license server is down.
- Releases now provide macOS trial binaries for Apple Silicon.

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
