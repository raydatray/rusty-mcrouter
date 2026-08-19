---
status: partial
created: 2026-08-16
updated: 2026-08-18
parent: 0001-observability.md
reference: ../reference/stats.md
---

# 0001 appendix: upstream stat catalog and port decisions

every stat upstream mcrouter defines (stat_list.h @ ~/mc/mcrouter, 232
entries + 76 carbon per-command names + per-pool + generated sections),
with a decision each. this is the faithfulness ledger for design 0001:
"10 metrics vs 232 stats" resolves here.

`port` and `fold` describe metric translation decisions. the parent
design's slice-status table says which areas are complete; config and
remaining event domains are not complete. feature-dependent `defer`
decisions remain deferred.

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
2. `_all` variants (include failover/shadow traffic) fold into backend
   attempt counters. normal-vs-failover behavior is represented by
   config-bounded pool attempt/final metrics, not a `leg` label on the
   command/result matrix.
3. max / max_max stats → not ported; `max_over_time()` in promql over
   scraped gauges. loses sub-scrape-interval peaks; accepted.

## process / identity

| upstream | decision | notes |
|----------|----------|-------|
| version, time, uptime | port | `rusty_mcrouter_build_info` + `rusty_mcrouter_start_time_seconds`; uptime is promql |
| commandargs, pid, parent_pid | defer | no matching exported family yet |
| ps_rss, ps_vsize, ps_num_{minor,major}_faults, ps_{user,system}_time_sec, rusage_system, rusage_user | defer | use a stock `process_*` collector when process metrics are added |
| fibers_allocated, fibers_pool_size, fibers_stack_high_watermark | n/a | no fibers; tokio task metrics are a different question (tokio-metrics, someday) |

## connections & servers

| upstream | decision | notes |
|----------|----------|-------|
| num_client_connections | port | gauge `rusty_mcrouter_client_connections` |
| num_servers{,_new,_up,_down,_closed} | defer | destination-up and TKO metrics ship; aggregate server-state families do not |
| num_suspect_servers | port | gauge, straight from `sus_servers` scan |
| num_connections_opened / _closed | port | counters; we already count connects + idle_closes per destination |
| num_connect_retries, num_connect_success_after_retrying | port | counters; connect-retry path exists |
| num_ssl_* (10), num_tls_to_plain_* (5), num_ktls_* (5) | n/a | no TLS. revisit wholesale if TLS ships |
| num_authorization_{failures,successes} | n/a | no auth |
| retrans_closed_connections, retrans_per_kbyte_{sum,max,avg}, retrans_num_total | n/a | kernel retransmit introspection; not planned |
| inactive_connection_closed_interval_sec | defer | idle-sweep exists, but this configuration gauge is not exported |

## tko / health

| upstream | decision | notes |
|----------|----------|-------|
| num_soft_tko_count, num_hard_tko_count | port | `rusty_mcrouter_tko{kind=}` gauges — GlobalTkoMetrics, already in 0001 |
| num_fail_open_state_{entered,exited} | port | counters read from live pool trackers; plus `rusty_mcrouter_pool_fail_open{pool=}` gauge |
| max_num_tko | promql | `max_over_time(rusty_mcrouter_tko[...])` |

## request results (backend leg)

upstream: `result_<err>` + `_all` + `_count` + `_all_count` for err ∈
{error, connect_error, connect_timeout, data_timeout, busy, tko,
client_error, local_error, remote_error, deadline_exceeded_error} = 40
names, plus final_result_error.

| upstream | decision | notes |
|----------|----------|-------|
| result_* (all 40) | fold | `rusty_mcrouter_backend_requests_total{command=,result=}` — our ResultCode enum is the result label; rates are promql |
| final_result_error | port | counter `rusty_mcrouter_requests_failed_total` — the *client-visible* error, distinct from per-attempt results |
| result_busy, result_deadline_exceeded_* | defer | no busy/deadline semantics yet; label values appear when the result codes do |

## request flow (frontend)

| upstream | decision | notes |
|----------|----------|-------|
| request_{sent,error,success,replied} + _count variants | fold | frontend `rusty_mcrouter_requests_total{command=}` plus backend/final error families; rates are promql |
| request_has_crypto_auth_token | n/a | |
| proxy_reqs_processing | port | `rusty_mcrouter_requests_processing` gauge; maps to slot map depth |
| proxy_reqs_waiting, proxy_request_num_outstanding | defer | no distinct queue-depth facts yet |
| proxy_queue_full, proxy_queues_all_full | fold | backend actor shedding is `rusty_mcrouter_backend_queue_full_total`; no separate frontend queue-full family |
| proxy_cpu, proxy_cpu_enabled | defer | needs a cpu-sampling loop; not phase 1 |
| num_proxies | port | trivial gauge |
| dev_null_requests | port | counter; NullRoute exists |
| duration_us, duration_get_us, duration_update_us, processing_time_us | defer | per-pool attempt and total route duration sums ship; no global per-op duration family yet |
| client_queue_notifications, client_queue_notify_period | n/a | vestigial: wake-up batching stats for upstream's client→proxy notification queue (originally the pre-folly "asox" queue); our channels have no equivalent knob |
| request_deadline_num_copy | n/a | deadline propagation internals |

## per-command (carbon codegen, 76 names)

`cmd_<c>_count`, `cmd_<c>`, `cmd_<c>_out`, `cmd_<c>_out_all` for 19
ascii commands.

| upstream | decision | notes |
|----------|----------|-------|
| all 76 | fold | `rusty_mcrouter_requests_total{command=}` (frontend) + `rusty_mcrouter_backend_requests_total{command=,result=}`. routed commands are the meta five; mn is `rusty_mcrouter_noops_total` |

## failover

| upstream | decision | notes |
|----------|----------|-------|
| failover_all, failover_all_failed(+_count) | port | `rusty_mcrouter_failover_total`, `rusty_mcrouter_failover_exhausted_total`; entry counts once per route decision, exhaustion means the terminal policy candidate errored |
| failover_inorder_policy(+_failed), failover_least_failures_policy(+_failed) | fold | `{policy="inorder|least_failures"}` — both policies exist |
| failover_deterministic_order_*, failover_rendezvous_*, failover_custom_* (17 names), custom_policy_attempts*, failover_conditional* | defer | policies we don't have; label values appear with the policy |
| failover_num_collisions, failover_num_failed_domain_collisions, failover_same_failure_domain, dest_with_no_failure_domain_count | defer | failure domains not implemented |
| failover_policy_result_error, failover_policy_tko_error | port | `rusty_mcrouter_failover_policy_errors_total{class="result|tko"}`; terminal-candidate errors are exhaustion, not policy errors |
| failover_rate_limited | defer | no failover rate limiting yet |

upstream anchors: `mcrouter/routes/FailoverRoute.h:193-286,337-361`,
`mcrouter/ProxyRequestContextTyped.h:109-115,164-191`, and
`mcrouter/routes/DestinationRoute.h:171-184`.

## destination batching / socket

| upstream | decision | notes |
|----------|----------|-------|
| destination_batches_sum, destination_requests_sum, destination_batch_size | port | we batch writes (drain_channel → one write_all); `rusty_mcrouter_backend_write_batches_total` + `_batched_requests_total`; avg batch size is promql |
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
| config_age, config_last_attempt, config_last_success, config_failures, configs_from_disk, config_full_attempt | defer | port with hot reload (design 000N); prometheus-shape: `rusty_mcrouter_config_last_success_timestamp_seconds` etc |
| config_age_sr, config_last_sr_update | n/a | servicerouter |

## asynclog / distribution / axon / acl / misc

| upstream | decision | notes |
|----------|----------|-------|
| asynclog_requests_rate, asynclog_spool_success_rate, asynclog_duration_us | defer | asynclog parsed but unimplemented; port if it ships |
| axon_proxy_* (4), distribution_* (13), srroute_error_on_delete_failure | n/a | meta-internal replication/distribution infra |
| prefix_acl_* (44 EXTERNAL_STAT) | n/a | meta acl infra |
| rim_report_failed | n/a | |
| rate_limited_log_count | port | maps to `rusty_mcrouter_events_dropped_total` (0001) — same job, our bus |
| load_balancer_load_reset_count | defer | no load-aware selection |
| before/after/total_latency_injected (5) | defer | fault injection; revisit with the DST work |
| redirected_lease_set_count | n/a | leases are ascii-protocol; no meta equivalent |

## per-pool (PoolStats)

| upstream | decision | notes |
|----------|----------|-------|
| `<pool>.requests.sum` | port | `rusty_mcrouter_pool_requests_total{pool=}` |
| `<pool>.final_result_error.sum` | port | `rusty_mcrouter_pool_requests_failed_total{pool=}` with `pool_completed_requests_total` as denominator |
| `<pool>.connections` | defer | no pool connection gauge ships |
| `<pool>.duration_us.avg`, `<pool>.total_duration_us.avg` | port | `pool_duration_us_sum_total` is per attempt; `pool_total_duration_us_sum_total` is final whole-route time; means are promql |

pool identity is resolved to a stable index during route construction.
every reached destination records an attempt; only a destination that
passes the backend TKO gate can claim final attribution. the first such
pool receives one completion/error/total-duration update when the
top-level route finishes, and later failover pools do not overwrite it.

upstream anchors: `mcrouter/PoolStats.h:19-103`,
`mcrouter/ProxyRequestContextTyped.h:109-115,164-191`,
`mcrouter/routes/DestinationRoute.h:171-184`, and
`mcrouter/ProxyRequestContext.h:110-114`.

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

## closure

the parent design's implemented inventory is the authoritative list of
exported names and labels. this appendix remains the exhaustive mapping
from upstream concepts to that smaller surface. deferred items must be
revisited by the design that introduces the corresponding feature (hot
reload, shadowing, custom failover, asynclog, fault injection, and so
on), with a metrics section in that design's checklist.
