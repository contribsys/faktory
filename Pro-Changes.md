# Faktory Pro Changelog

Changelog: [Faktory](https://github.com/contribsys/faktory/blob/master/Changes.md) || Faktory Pro || [Faktory Enterprise](https://github.com/contribsys/faktory/blob/master/Ent-Changes.md)

A trial version of Faktory Pro for OSX is available with each [release](/contribsys/faktory/releases/).
Click to purchase [Faktory Pro](https://billing.contribsys.com/fpro/).

## 1.4.2

- *No significant changes*

## 1.4.1

- *No significant changes*

## 1.4.0

- `-e staging` environment support, limited to 100 connections
- Fixes for linter problems

## 1.3.0

* Validate statsd tags, per Datadog docs [#283]
* Add periodic job history and details page [#265]
* Allow cron jobs to specify static arguments:
```toml
[[cron]]
  schedule = "* * * * *"
  [cron.job]
    type = "SomeWorker"
    args = ["minutely"]

[[cron]]
  schedule = "0 * * * *"
  [cron.job]
    type = "SomeWorker"
    args = ["hourly"]
```

## 1.2.0

- **BREAKING** Deprecate some statsd metrics, see issue for details [#261]
- Add some new statsd metrics for job expiration, uniqueness and cron. [#261]

## 1.1.0

- The canonical copy of each cron job was mutable, leading to
  inconsistent cron jobs over time [#234]
- Fix display of Next Run to use UTC [#234]

## 1.0.1

- Fix unique jobs cannot be retried [#220]

## 1.0.0

- Allow for TOML-defined queue latency metrics [#206]
- Better middleware error handling [#208]

## 0.9.7

- Add [Statsd metrics](/contribsys/faktory/wiki/Pro-Metrics) feature

## 0.9.6

- Fix non-deterministic unique locks when jobs have map arguments. [#203]

## 0.9.5

- No changes.

## 0.9.4

- Fix for unique locks not releasing upon job success [#194]

## 0.9.3

- Add [Unique Jobs](/contribsys/faktory/wiki/Pro-Unique_Jobs) feature [#194]

## 0.9.2

- First [Faktory Pro](https://contribsys.com/faktory) release
