# Research methodology and limits

## Question

How did ten selected U.S. assets behave around 202 official CPI, Employment, PCE and FOMC releases, using market and macro information that can be tied to the release time?

## Reproducible identities

- market source/feed: `alpaca / sip`
- analysis version: `multi_event_sip_v1`
- windows: `PRE_60M`, `POST_5M`, `POST_30M`, `POST_60M`
- baseline: `pre60_momentum_post60 / v1`
- assumed round-trip cost: 10 bp

The API returns these identities rather than querying an arbitrary latest version.

## Coverage

Collection completion describes HTTP success, requested bounds, normal pagination termination, no max-page truncation and valid provider data. Observed bar coverage describes which candidate timestamps contain provider price bars. Analysis eligibility keeps the existing 90% rule. A missing provider bar is not forward-filled and does not alone prove collection failure.

## Baseline result

The simple baseline follows the pre-release 60-minute direction and exits 60 minutes after release. Among 2,020 event–asset observations, 1,988 have a calculable net result. The mean after 10 bp is **-0.1565%** and the positive rate is **39.34%**.

This is a deliberately unsuccessful exploratory baseline. It is not a forecast, portfolio return, causal estimate or trading recommendation.

## Known limits

- release forecast, first-release actual and point-in-time surprise are not yet available for all event types;
- non-event controls and confounder adjustment remain future work;
- SIP minute bars can be sparse and do not represent order-book liquidity;
- assumed transaction cost is not a fill simulation;
- multiple assets and releases are not combined into a portfolio process.
