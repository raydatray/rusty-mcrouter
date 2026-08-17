---
status: draft
created: 2026-08-16
parent: 0001-observability.md
reference: ../reference/stats.md
---

# 0001 appendix: upstream stat catalog and port decisions

every stat upstream mcrouter defines (stat_list.h @ ~/mc/mcrouter, 232
entries + 76 carbon per-command names + per-pool + generated sections),
with a decision each. this is the faithfulness ledger for design 0001:
"10 metrics vs 232 stats" resolves here.

## decision vocabulary

| decision   | meaning |
|------------|---------|
| **port**   | export as (part of) a prometheus metric family |
| **fold**   | absorbed into an existing family as a label value — no new metric |
| **promql** | derivable at query time from ported counters (rates, averages); recording rule if wanted |
| **process**| provided by a stock process collector, not hand-ported |
| **defer**  | feature doesn't exist in rusty-mcrouter yet; port when it does (noted) |
| **n/a**    | meta-internal or deliberately-not-implemented feature; will not port |

blanket rules applied throughout:

1. every upstream `<x>` (windowed rate) + `<x>_count` / `<x>_all_count`
   (cumulative) pair → **one** monotonic counter; the rate is promql.
   this alone collapses ~60 names.
2. `_all` variants (include failover/shadow traffic) → a label
   (`leg="normal|failover"`), not a second family.
3. max / max_max stats → not ported; `max_over_time()` in promql over
   scraped gauges. loses sub-scrape-interval peaks; accepted.

## process / identity

| upstream | decision | notes |
|----------|----------|-------|
| version, commandargs, pid, parent_pid, time, uptime | port | `mcrouter_build_info` info-gauge + `mcrouter_start_time_seconds`; uptime is promql |
| ps_rss, ps_vsize, ps_num_{minor,major}_faults, ps_{user,system}_time_sec, rusage_system, rusage_user | process | stock `process_*` collector |
| fibers_allocated, fibers_pool_size, fibers_stack_high_watermark | n/a | no fibers; tokio task metrics are a different question (tokio-metrics, someday) |

## connections & servers

| upstream | decision | notes |
|----------|----------|-------|
| num_client_connections | port | gauge `mcrouter_client_connections` |
| num_servers{,_new,_up,_down,_closed} | port | gauge `mcrouter_servers{state=}` |
| num_suspect_servers | port | gauge, straight from `sus_servers` scan |
| num_connections_opened / _closed | port | counters; we already count connects + idle_closes per destination |
| num_connect_retries, num_connect_success_after_retrying | port | counters; connect-retry path exists |
| num_ssl_* (10), num_tls_to_plain_* (5), num_ktls_* (5) | n/a | no TLS. revisit wholesale if TLS ships |
| num_authorization_{failures,successes} | n/a | no auth |
| retrans_closed_connections, retrans_per_kbyte_{sum,max,avg}, retrans_num_total | n/a | kernel retransmit introspection; not planned |
| inactive_connection_closed_interval_sec | port | idle-sweep already exists; expose as gauge or drop — decide in slice 3 |

## tko / health

| upstream | decision | notes |
|----------|----------|-------|
| num_soft_tko_count, num_hard_tko_count | port | `mcrouter_tko{kind=}` gauges — TkoCounters, already in 0001 |
| num_fail_open_state_{entered,exited} | port | counters from EnterFailOpen/ExitFailOpen events; plus `mcrouter_pool_fail_open{pool=}` gauge (0001) |
| max_num_tko | promql | `max_over_time(mcrouter_tko[...])` |

## request results (backend leg)

upstream: `result_<err>` + `_all` + `_count` + `_all_count` for err ∈
{error, connect_error, connect_timeout, data_timeout, busy, tko,
client_error, local_error, remote_error, deadline_exceeded_error} = 40
names, plus final_result_error.

| upstream | decision | notes |
|----------|----------|-------|
| result_* (all 40) | fold | `mcrouter_backend_requests_total{result=, leg=}` — our ResultCode enum is the label; rates are promql |
| final_result_error | port | counter `mcrouter_requests_failed_total` — the *client-visible* error, distinct from per-attempt results |
| result_busy, result_deadline_exceeded_* | defer | no busy/deadline semantics yet; label values appear when the result codes do |

## request flow (frontend)

| upstream | decision | notes |
|----------|----------|-------|
| request_{sent,error,success,replied} + _count variants | fold | `mcrouter_requests_total{...}` + result labels; rates promql |
| request_has_crypto_auth_token | n/a | |
| proxy_reqs_processing, proxy_reqs_waiting, proxy_request_num_outstanding | port | gauges; maps to our slot map depth — good early-warning signals |
| proxy_queue_full, proxy_queues_all_full | port | counters; maps to connection-actor QueueFull shedding |
| proxy_cpu, proxy_cpu_enabled | defer | needs a cpu-sampling loop; not phase 1 |
| num_proxies | port | trivial gauge |
| dev_null_requests | port | counter; NullRoute exists |
| duration_us, duration_get_us, duration_update_us, processing_time_us | port | as monotonic µs sums, mean via promql (not upstream's EWMA — a snapshot-export artifact); histogram question open in 0001 |
| client_queue_notifications, client_queue_notify_period | n/a | vestigial: wake-up batching stats for upstream's client→proxy notification queue (originally the pre-folly "asox" queue); our channels have no equivalent knob |
| request_deadline_num_copy | n/a | deadline propagation internals |

## per-command (carbon codegen, 76 names)

`cmd_<c>_count`, `cmd_<c>`, `cmd_<c>_out`, `cmd_<c>_out_all` for 19
ascii commands.

| upstream | decision | notes |
|----------|----------|-------|
| all 76 | fold | `mcrouter_requests_total{command=}` (frontend) + `mcrouter_backend_requests_total{command=, leg=}`. our command set is the meta five (mg/ms/md/ma/me) + mn — 19 ascii commands don't exist here; the *shape* ports, the cardinality shrinks |

## failover

| upstream | decision | notes |
|----------|----------|-------|
| failover_all, failover_all_failed(+_count) | port | `mcrouter_failover_total`, `mcrouter_failover_exhausted_total` |
| failover_inorder_policy(+_failed), failover_least_failures_policy(+_failed) | fold | `mcrouter_failover_total{policy=}` — both policies exist |
| failover_deterministic_order_*, failover_rendezvous_*, failover_custom_* (17 names), custom_policy_attempts*, failover_conditional* | defer | policies we don't have; label values appear with the policy |
| failover_num_collisions, failover_num_failed_domain_collisions, failover_same_failure_domain, dest_with_no_failure_domain_count | defer | failure domains not implemented |
| failover_policy_result_error, failover_policy_tko_error | port | counters; maps to our route_code classification |
| failover_rate_limited | defer | no failover rate limiting yet |

## destination batching / socket

| upstream | decision | notes |
|----------|----------|-------|
| destination_batches_sum, destination_requests_sum, destination_batch_size | port | we batch writes (drain_channel → one write_all); `mcrouter_backend_write_batches_total` + `_requests_total`; avg batch size is promql |
| destination_pending_reqs, destination_inflight_reqs | port | gauges — pending/inflight VecDeque depths, cheap and valuable |
| destination_max_{pending,inflight}_reqs | promql | max_over_time |
| destination_inflight_shadow_reqs (+max) | defer | shadow routes |
| destination_reqs_dirty_buffer_*, destination_reqs_total_sum | n/a | write-buffer reuse accounting internal to upstream's client implementation; our write path (encode into one buf, single write_all) has no analogous state |
| num_socket_writes, num_socket_partial_writes | n/a | upstream observes raw nonblocking write syscalls and their short-write partials; our write path is `write_all` over one batch buffer, so writes ≈ `write_batches` (already ported) and partials are not observable |
| replies_compressed, replies_not_compressed, reply_traffic_{before,after}_compression | n/a | no compression |

## outstanding-request queues

| upstream | decision | notes |
|----------|----------|-------|
| outstanding_route_{get,update}_reqs_queued (+_helper, wait_time sums, avg_queue_size, avg_wait_time) — 10 names | n/a | upstream's outstanding-limit fiber queue; our backpressure is the bounded actor channel, covered by proxy_queue_full above |

## config

| upstream | decision | notes |
|----------|----------|-------|
| config_age, config_last_attempt, config_last_success, config_failures, configs_from_disk, config_full_attempt | defer | port with hot reload (design 000N); prometheus-shape: `mcrouter_config_last_success_timestamp_seconds` etc |
| config_age_sr, config_last_sr_update | n/a | servicerouter |

## asynclog / distribution / axon / acl / misc

| upstream | decision | notes |
|----------|----------|-------|
| asynclog_requests_rate, asynclog_spool_success_rate, asynclog_duration_us | defer | asynclog parsed but unimplemented; port if it ships |
| axon_proxy_* (4), distribution_* (13), srroute_error_on_delete_failure | n/a | meta-internal replication/distribution infra |
| prefix_acl_* (44 EXTERNAL_STAT) | n/a | meta acl infra |
| rim_report_failed | n/a | |
| rate_limited_log_count | port | maps to `mcrouter_events_dropped_total` (0001) — same job, our bus |
| load_balancer_load_reset_count | defer | no load-aware selection |
| before/after/total_latency_injected (5) | defer | fault injection; revisit with the DST work |
| redirected_lease_set_count | n/a | leases are ascii-protocol; no meta equivalent |

## per-pool (PoolStats)

| upstream | decision | notes |
|----------|----------|-------|
| `<pool>.requests.sum` | port | `mcrouter_pool_requests_total{pool=}` |
| `<pool>.final_result_error.sum` | port | `{pool=}` label on requests_failed |
| `<pool>.connections` | port | gauge `{pool=}` |
| `<pool>.duration_us.avg`, `<pool>.total_duration_us.avg` | port | as monotonic µs sums, mean via promql |

pool cardinality is config-bounded (tens, not thousands) — safe as a
label, unlike per-destination.

## generated sections (`stats servers`, `stats suspect_servers`)

the protocol commands are **not ported** — no stats-command or
`__mcrouter__.` compat is planned; /metrics is the only observability
surface. the *data* ports into /metrics as the per-destination
families (design 0001): `destination_up` (tko state),
`destination_requests_total{result}`, `destination_latency_us_sum`,
`destination_inflight_reqs`. destination counts are config-bounded
and small at our scale, so this is cheap. what deliberately does not
port: the `destination × command × result` cube (command anomalies
are keyspace questions — the aggregate `{command, result}` family
answers them) and free-text detail (tko reason, probe state), which
lives in the event log. if snapshot debugging ever needs more, the
escape hatch is a JSON endpoint on the http listener, not protocol
compat.

## tally

| decision | count (approx names) |
|----------|----------------------|
| port     | ~55 upstream names → ~20 metric families |
| fold     | ~120 (results × variants, per-command) → labels on 2–3 families |
| promql   | ~15 |
| process  | ~8 |
| defer    | ~45 (config, shadow, custom failover, asynclog, fault injection) |
| n/a      | ~90 (tls/ssl, acl, axon/distribution, fibers, compression, retrans, leases) |

deferred items must be re-visited by the design doc of the feature
that unblocks them (hot reload → config stats, shadow → shadow
gauges, etc.) — add a "metrics" section to that design's checklist.
