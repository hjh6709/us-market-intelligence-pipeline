# Serving, deployment and product design

## Scope

Evolve the verified local FastAPI/browser surface into separately secured read, operations and Paper-experiment surfaces.

## Target interfaces

Read resources expose event facts, quality, reactions and provenance. Operations resources expose pipeline freshness and failures. Paper resources require authentication, experiment identity, explicit confirmation and a Paper-only broker configuration. Heavy raw replay runs outside request handling.

## Failure rules and tests

Unavailable data is explicit; it is not HTTP success with invented content. Authorization is deny-by-default. Broker POST is not automatically retried and uncertain results reconcile by client order ID. Contract, browser, accessibility and deployment smoke tests gate release.

## Delivery status

Documentation and plan only. The current local web application remains the implemented product surface; public production deployment is not claimed.
