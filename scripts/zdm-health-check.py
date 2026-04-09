#!/usr/bin/env python3
"""
ZDM Proxy health check script.

Scrapes the Prometheus metrics endpoint, checks for signs of trouble,
and sends alerts to Slack and/or PagerDuty when thresholds are breached.
No external dependencies — stdlib Python 3 only.

Usage:
    # One-shot check (stdout only)
    python3 zdm-health-check.py

    # Run every 60 seconds with Slack alerts
    python3 zdm-health-check.py --interval 60 --slack-webhook-url https://hooks.slack.com/services/XXX/YYY/ZZZ

    # Run with PagerDuty alerts
    python3 zdm-health-check.py --interval 60 --pagerduty-routing-key abc123...

    # Both Slack and PagerDuty
    python3 zdm-health-check.py --interval 60 \
        --slack-webhook-url https://hooks.slack.com/services/XXX/YYY/ZZZ \
        --pagerduty-routing-key abc123...

    # Custom thresholds
    python3 zdm-health-check.py --interval 60 --failed-writes-threshold 10 --write-timeout-threshold 10

    # With per-table write failure log
    python3 zdm-health-check.py --interval 60 --log-file /var/log/zdm-write-failures.log

Environment variables (all optional, CLI args take precedence):
    ZDM_METRICS_URL                - metrics endpoint (default: http://localhost:14001/metrics)
    ZDM_CHECK_INTERVAL             - seconds between checks when running in loop mode
    ZDM_FAILED_WRITES_THRESHOLD         - alert if failed writes (target/both) increase by more than this per interval (default: 5)
    ZDM_WRITE_TIMEOUT_THRESHOLD         - alert if target read failures increase by more than this per interval (default: 5)
    ZDM_FAILED_ORIGIN_WRITES_THRESHOLD  - alert if origin write failures increase by more than this per interval (default: 0)
    ZDM_ZERO_CLIENTS_INTERVALS          - alert if client connections stay at 0 for this many consecutive intervals (default: 2)
    ZDM_P99_LATENCY_THRESHOLD_MS        - alert if estimated p99 latency exceeds this value in ms (default: 500)
    ZDM_SLACK_WEBHOOK_URL               - Slack incoming webhook URL
    ZDM_PAGERDUTY_ROUTING_KEY      - PagerDuty Events API v2 integration/routing key
    ZDM_PAGERDUTY_SOURCE           - source field for PagerDuty events (default: zdm-proxy)
    ZDM_WRITE_FAILURES_LOG         - path to per-table write failure log file (default: zdm-write-failures.log)
"""

import argparse
import json
import os
import signal
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

MAX_RESPONSE_BYTES = 1024 * 1024  # 1MB safety cap on metrics response
SLACK_COOLDOWN_SECONDS = 300      # Don't send more than one Slack alert per 5 minutes

WRITE_SUCCESS_METRIC = "zdm_proxy_write_success_total"


# ---------------------------------------------------------------------------
# Metrics parsing
# ---------------------------------------------------------------------------

def parse_prometheus_text(text):
    """Parse Prometheus text exposition format into {metric_name{labels}: value}."""
    metrics = {}
    for line in text.strip().split("\n"):
        if line.startswith("#") or not line.strip():
            continue
        try:
            parts = line.rsplit(" ", 1)
            metrics[parts[0].strip()] = float(parts[1].strip())
        except (IndexError, ValueError):
            continue
    return metrics


def fetch_metrics(url):
    """Fetch and parse metrics from the ZDM proxy. Returns (dict, error_string)."""
    try:
        req = urllib.request.Request(url, headers={"Accept": "text/plain"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read(MAX_RESPONSE_BYTES)
            return parse_prometheus_text(body.decode("utf-8")), None
    except urllib.error.URLError as e:
        return None, f"Cannot reach metrics endpoint {url}: {e}"
    except Exception as e:
        return None, f"Error fetching metrics from {url}: {e}"


def get_metric(metrics, name, default=0.0):
    return metrics.get(name, default)


def sum_metrics_matching(metrics, prefix, label_filter):
    """Sum all metric values whose key starts with prefix and contains label_filter."""
    return sum(v for k, v in metrics.items() if k.startswith(prefix) and label_filter in k)


def get_histogram_buckets(metrics, metric_type):
    """Return sorted list of (le_float, value, original_key) for the given histogram type."""
    prefix = f'zdm_proxy_request_duration_seconds_bucket{{type="{metric_type}"'
    result = []
    for key, val in metrics.items():
        if not key.startswith(prefix):
            continue
        try:
            le_str = key.split(',le="')[1].rstrip('"}')
            le = float('inf') if le_str == '+Inf' else float(le_str)
            result.append((le, val, key))
        except (IndexError, ValueError):
            continue
    return sorted(result, key=lambda x: x[0])


def estimate_p99(metrics, prev_metrics, metric_type):
    """Estimate p99 latency in seconds for the given request type over the last interval.

    Returns the estimated p99 in seconds, or None if there were no requests this interval.
    """
    buckets = get_histogram_buckets(metrics, metric_type)
    if not buckets:
        return None

    if prev_metrics:
        delta_buckets = [
            (le, max(0.0, val - prev_metrics.get(key, 0.0)), key)
            for le, val, key in buckets
        ]
    else:
        delta_buckets = buckets

    total = next((d for le, d, _ in delta_buckets if le == float('inf')), 0.0)
    if total == 0:
        return None

    target = 0.99 * total
    prev_count = 0.0
    prev_le = 0.0
    for le, count, _ in delta_buckets:
        if le == float('inf'):
            break
        if count >= target:
            if count == prev_count:
                return le
            frac = (target - prev_count) / (count - prev_count)
            return prev_le + frac * (le - prev_le)
        prev_count = count
        prev_le = le

    # p99 falls in the last finite bucket
    last_finite = next((le for le, _, _ in reversed(delta_buckets) if le != float('inf')), None)
    return last_finite


# ---------------------------------------------------------------------------
# Per-table write failure detection
# ---------------------------------------------------------------------------

def parse_metric_labels(key):
    """Parse label key-value pairs from a Prometheus metric key string.

    Example: 'zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="users"}'
    Returns: {'cluster': 'origin', 'keyspace': 'ks1', 'table': 'users'}
    """
    labels = {}
    brace_start = key.find('{')
    if brace_start == -1:
        return labels
    inner = key[brace_start + 1:].rstrip('}')
    for part in inner.split(','):
        if '=' not in part:
            continue
        k, _, v = part.partition('=')
        labels[k.strip()] = v.strip().strip('"')
    return labels


def extract_write_success_per_table(metrics):
    """Return {(cluster, keyspace, table): float} from zdm_proxy_write_success_total metrics."""
    result = {}
    prefix = WRITE_SUCCESS_METRIC + "{"
    for key, val in metrics.items():
        if not key.startswith(prefix):
            continue
        lbls = parse_metric_labels(key)
        cluster = lbls.get("cluster", "")
        keyspace = lbls.get("keyspace", "")
        table = lbls.get("table", "")
        if cluster and keyspace and table:
            result[(cluster, keyspace, table)] = val
    return result


def detect_per_table_write_failures(metrics, prev_metrics):
    """Detect per-table write failures by comparing origin vs target write success deltas.

    When a write reaches the proxy it is forwarded to BOTH clusters. If the write
    succeeds on origin but not on target, origin's counter increments while target's
    does not — the difference is the number of failed writes for that table.
    The reverse holds for origin-only failures.

    Returns list of (side, keyspace, table, failed_count) where:
      side="target"  → writes succeeded on origin but FAILED on target
      side="origin"  → writes succeeded on target but FAILED on origin

    Note: writes that fail on BOTH clusters leave neither counter incremented, so
    they cannot be attributed to a specific table here — they are captured by the
    aggregate zdm_proxy_failed_writes_total{failed_on="both"} metric instead.
    """
    if prev_metrics is None:
        return []

    cur_table = extract_write_success_per_table(metrics)
    prev_table = extract_write_success_per_table(prev_metrics)

    combos = set()
    for cluster, ks, tbl in cur_table:
        combos.add((ks, tbl))
    for cluster, ks, tbl in prev_table:
        combos.add((ks, tbl))

    failures = []
    for ks, tbl in sorted(combos):
        origin_delta = max(0.0, cur_table.get(("origin", ks, tbl), 0.0)
                               - prev_table.get(("origin", ks, tbl), 0.0))
        target_delta = max(0.0, cur_table.get(("target", ks, tbl), 0.0)
                               - prev_table.get(("target", ks, tbl), 0.0))

        # More origin successes than target successes → target had failures
        if origin_delta > target_delta:
            failures.append(("target", ks, tbl, int(origin_delta - target_delta)))

        # More target successes than origin successes → origin had failures
        elif target_delta > origin_delta:
            failures.append(("origin", ks, tbl, int(target_delta - origin_delta)))

    return failures


def log_write_failures(log_file, failures):
    """Append per-table write failure entries to the log file.

    Log format per line:
        YYYY-MM-DD HH:MM:SS UTC  origin|target  keyspace.table  N failed writes
    """
    if not log_file or not failures:
        return
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        with open(log_file, "a") as f:
            for side, ks, tbl, count in failures:
                f.write(f"{ts}  {side}  {ks}.{tbl}  {count} failed writes\n")
    except OSError as e:
        print(f"  Failed to write to log file {log_file}: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def check_health(metrics, prev_metrics, config, zero_clients_streak=0):
    """Compare current vs previous metrics snapshot. Returns list of (severity, message) tuples."""
    problems = []

    if metrics is None:
        return [("critical", "Metrics endpoint is unreachable - proxy may be down")]

    def delta(current, previous):
        d = current - previous
        if d < 0:
            # Counter reset (proxy restarted) — skip this interval
            return 0
        return d

    # Failed writes on target / both
    for label in ["target", "both"]:
        key = f'zdm_proxy_failed_writes_total{{failed_on="{label}"}}'
        cur = get_metric(metrics, key)
        prev = get_metric(prev_metrics, key) if prev_metrics else cur
        d = delta(cur, prev)
        if d > config["failed_writes_threshold"]:
            problems.append(("warning", f"Failed writes (failed_on={label}): +{int(d)} in last interval (total: {int(cur)})"))

    # Origin write failures — any increase is critical (origin is the source of truth)
    cur = get_metric(metrics, 'zdm_proxy_failed_writes_total{failed_on="origin"}')
    prev = get_metric(prev_metrics, 'zdm_proxy_failed_writes_total{failed_on="origin"}') if prev_metrics else cur
    d = delta(cur, prev)
    if d > config["failed_origin_writes_threshold"]:
        problems.append(("critical", f"ORIGIN write failures: +{int(d)} in last interval (total: {int(cur)}) — data may not be reaching Astra"))

    # Target read failures
    cur = get_metric(metrics, 'zdm_proxy_failed_reads_total{cluster="target"}')
    prev = get_metric(prev_metrics, 'zdm_proxy_failed_reads_total{cluster="target"}') if prev_metrics else cur
    d = delta(cur, prev)
    if d > config["write_timeout_threshold"]:
        problems.append(("warning", f"Target read failures: +{int(d)} in last interval (total: {int(cur)})"))

    # Origin read failures — any at all is critical
    cur = get_metric(metrics, 'zdm_proxy_failed_reads_total{cluster="origin"}')
    prev = get_metric(prev_metrics, 'zdm_proxy_failed_reads_total{cluster="origin"}') if prev_metrics else cur
    d = delta(cur, prev)
    if d > 0:
        problems.append(("critical", f"ORIGIN read failures: +{int(d)} in last interval (total: {int(cur)})"))

    # Connection failures
    for cluster in ["origin", "target"]:
        cur = get_metric(metrics, f'zdm_proxy_failed_connections_total{{cluster="{cluster}"}}')
        prev = get_metric(prev_metrics, f'zdm_proxy_failed_connections_total{{cluster="{cluster}"}}') if prev_metrics else cur
        d = delta(cur, prev)
        if d > 3:
            problems.append(("warning", f"{cluster.title()} connection failures: +{int(d)} in last interval"))

    # Zero client connections — sustained absence of clients means the app stopped talking to the proxy
    if zero_clients_streak >= config["zero_clients_intervals"]:
        problems.append(("critical",
            f"No client connections for {zero_clients_streak} consecutive intervals — "
            f"application may have stopped routing through the proxy"))

    # P99 latency check
    threshold_s = config["p99_latency_threshold_ms"] / 1000.0
    for req_type in ["writes", "reads_origin", "reads_target"]:
        p99 = estimate_p99(metrics, prev_metrics, req_type)
        if p99 is not None and p99 > threshold_s:
            problems.append(("warning",
                f"High p99 latency for {req_type}: {p99 * 1000:.0f}ms "
                f"(threshold: {config['p99_latency_threshold_ms']}ms)"))

    # Per-table write failures (derived from write success counter divergence)
    per_table = detect_per_table_write_failures(metrics, prev_metrics)
    for side, ks, tbl, count in per_table:
        if side == "origin":
            severity = "critical"
            detail = "writes reached target but FAILED on origin — source-of-truth may be losing data"
        else:
            severity = "warning"
            detail = "writes reached origin but FAILED on target — migration target is falling behind"
        problems.append((severity,
            f"Per-table write failure on {side}: {ks}.{tbl} +{count} failed writes ({detail})"))

    return problems, per_table


# ---------------------------------------------------------------------------
# Alerting — Slack
# ---------------------------------------------------------------------------

_last_slack_alert_time = 0.0


def send_slack(webhook_url, problems, metrics_url):
    global _last_slack_alert_time
    if not webhook_url:
        return

    # Rate-limit Slack messages to avoid alert storms during sustained outages
    elapsed = time.monotonic() - _last_slack_alert_time
    if _last_slack_alert_time > 0 and elapsed < SLACK_COOLDOWN_SECONDS:
        print(f"  Slack alert suppressed (cooldown, {int(SLACK_COOLDOWN_SECONDS - elapsed)}s remaining)")
        return

    worst = "critical" if any(s == "critical" for s, _ in problems) else "warning"
    emoji = ":rotating_light:" if worst == "critical" else ":warning:"

    text = f"{emoji} *ZDM Proxy Alert*\n*Host:* `{metrics_url}`\n*Time:* {now()}\n"
    for severity, msg in problems:
        icon = ":red_circle:" if severity == "critical" else ":large_orange_circle:"
        text += f"{icon} {msg}\n"

    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(webhook_url, data=payload,
                                headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 204):
                print(f"  Slack webhook returned status {resp.status}", file=sys.stderr)
            else:
                _last_slack_alert_time = time.monotonic()
    except Exception as e:
        print(f"  Failed to send Slack alert: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Alerting — PagerDuty Events API v2
# ---------------------------------------------------------------------------

PAGERDUTY_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"


def send_pagerduty(routing_key, source, problems, metrics_url):
    if not routing_key:
        return

    worst = "critical" if any(s == "critical" for s, _ in problems) else "warning"

    summary_parts = []
    for _, msg in problems:
        summary_parts.append(msg)
    summary = f"ZDM Proxy: {'; '.join(summary_parts)}"
    # PagerDuty summary max 1024 chars
    if len(summary) > 1024:
        summary = summary[:1021] + "..."

    payload = {
        "routing_key": routing_key,
        "event_action": "trigger",
        "dedup_key": f"zdm-proxy-health-{metrics_url}",
        "payload": {
            "summary": summary,
            "source": source,
            "severity": worst,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "component": "zdm-proxy",
            "group": "cassandra-migration",
            "class": "health-check",
            "custom_details": {
                "metrics_url": metrics_url,
                "problems": [{"severity": s, "message": m} for s, m in problems],
            },
        },
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(PAGERDUTY_EVENTS_URL, data=data,
                                headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            if resp.status == 202:
                result = json.loads(body)
                print(f"  PagerDuty alert triggered: {result.get('dedup_key', 'unknown')}")
            else:
                print(f"  PagerDuty returned status {resp.status}: {body}", file=sys.stderr)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8") if e.fp else ""
        print(f"  PagerDuty error {e.code}: {body}", file=sys.stderr)
    except Exception as e:
        print(f"  Failed to send PagerDuty alert: {e}", file=sys.stderr)


def resolve_pagerduty(routing_key, metrics_url):
    """Send a resolve event to auto-close the PagerDuty incident when things recover."""
    if not routing_key:
        return

    payload = {
        "routing_key": routing_key,
        "event_action": "resolve",
        "dedup_key": f"zdm-proxy-health-{metrics_url}",
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(PAGERDUTY_EVENTS_URL, data=data,
                                headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10):
            print(f"  PagerDuty incident resolved")
    except Exception as e:
        print(f"  Failed to resolve PagerDuty incident: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_summary(metrics):
    if metrics is None:
        print(f"[{now()}] UNREACHABLE")
        return

    writes_target = get_metric(metrics, 'zdm_proxy_failed_writes_total{failed_on="target"}')
    writes_origin = get_metric(metrics, 'zdm_proxy_failed_writes_total{failed_on="origin"}')
    writes_both = get_metric(metrics, 'zdm_proxy_failed_writes_total{failed_on="both"}')
    clients = get_metric(metrics, "zdm_client_connections_total")
    inflight = get_metric(metrics, 'zdm_proxy_inflight_requests_total{type="writes"}')

    print(
        f"[{now()}] OK | "
        f"clients={int(clients)} "
        f"inflight_writes={int(inflight)} "
        f"failed_writes: origin={int(writes_origin)} target={int(writes_target)} both={int(writes_both)}"
    )


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ZDM Proxy health checker with Slack and PagerDuty alerting")

    parser.add_argument("--metrics-url",
                        default=os.environ.get("ZDM_METRICS_URL", "http://localhost:14001/metrics"),
                        help="ZDM proxy metrics endpoint")
    parser.add_argument("--interval", type=int,
                        default=int(os.environ.get("ZDM_CHECK_INTERVAL", "0")),
                        help="Seconds between checks. 0 = one-shot.")
    parser.add_argument("--failed-writes-threshold", type=int,
                        default=int(os.environ.get("ZDM_FAILED_WRITES_THRESHOLD", "5")),
                        help="Alert if failed writes (target/both) increase by more than this per interval")
    parser.add_argument("--write-timeout-threshold", type=int,
                        default=int(os.environ.get("ZDM_WRITE_TIMEOUT_THRESHOLD", "5")),
                        help="Alert if target read failures increase by more than this per interval")
    parser.add_argument("--failed-origin-writes-threshold", type=int,
                        default=int(os.environ.get("ZDM_FAILED_ORIGIN_WRITES_THRESHOLD", "0")),
                        help="Alert if origin write failures increase by more than this per interval (default: 0)")
    parser.add_argument("--zero-clients-intervals", type=int,
                        default=int(os.environ.get("ZDM_ZERO_CLIENTS_INTERVALS", "2")),
                        help="Alert if client connections stay at 0 for this many consecutive intervals (default: 2)")
    parser.add_argument("--p99-latency-threshold-ms", type=int,
                        default=int(os.environ.get("ZDM_P99_LATENCY_THRESHOLD_MS", "500")),
                        help="Alert if estimated p99 latency exceeds this value in ms (default: 500)")
    parser.add_argument("--log-file",
                        default=os.environ.get("ZDM_WRITE_FAILURES_LOG", "zdm-write-failures.log"),
                        help="Path to per-table write failure log file. Set to empty string to disable.")

    # Slack
    parser.add_argument("--slack-webhook-url",
                        default=os.environ.get("ZDM_SLACK_WEBHOOK_URL", ""),
                        help="Slack incoming webhook URL")

    # PagerDuty
    parser.add_argument("--pagerduty-routing-key",
                        default=os.environ.get("ZDM_PAGERDUTY_ROUTING_KEY", ""),
                        help="PagerDuty Events API v2 integration/routing key")
    parser.add_argument("--pagerduty-source",
                        default=os.environ.get("ZDM_PAGERDUTY_SOURCE", "zdm-proxy"),
                        help="Source field for PagerDuty events (default: zdm-proxy)")

    args = parser.parse_args()

    config = {
        "failed_writes_threshold": args.failed_writes_threshold,
        "write_timeout_threshold": args.write_timeout_threshold,
        "failed_origin_writes_threshold": args.failed_origin_writes_threshold,
        "zero_clients_intervals": args.zero_clients_intervals,
        "p99_latency_threshold_ms": args.p99_latency_threshold_ms,
    }

    log_file = args.log_file or ""

    has_alerting = bool(args.slack_webhook_url or args.pagerduty_routing_key)
    if not has_alerting:
        print("Warning: no alerting configured. Set --slack-webhook-url and/or --pagerduty-routing-key for alerts.",
              file=sys.stderr)

    if log_file:
        print(f"Per-table write failures will be logged to: {log_file}", file=sys.stderr)

    # Clean shutdown on SIGINT/SIGTERM
    def handle_signal(signum, frame):
        print(f"\n[{now()}] Shutting down.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    prev_metrics = None
    was_alerting = False
    zero_clients_streak = 0

    while True:
        metrics, err = fetch_metrics(args.metrics_url)

        if err:
            print(f"[{now()}] ERROR: {err}", file=sys.stderr)
            problems = [("critical", err)]
            per_table_failures = []
        else:
            clients = get_metric(metrics, "zdm_client_connections_total")
            if clients == 0:
                zero_clients_streak += 1
            else:
                zero_clients_streak = 0
            print_summary(metrics)
            problems, per_table_failures = check_health(metrics, prev_metrics, config, zero_clients_streak)

        if per_table_failures:
            log_write_failures(log_file, per_table_failures)
            for side, ks, tbl, count in per_table_failures:
                print(f"  WRITE FAILURE [{side}]: {ks}.{tbl} +{count} failed writes")

        if problems:
            for severity, msg in problems:
                # Per-table failures are printed above; skip reprinting them here
                if "Per-table write failure" not in msg:
                    print(f"  ALERT [{severity}]: {msg}")
            send_slack(args.slack_webhook_url, problems, args.metrics_url)
            send_pagerduty(args.pagerduty_routing_key, args.pagerduty_source, problems, args.metrics_url)
            was_alerting = True
        elif was_alerting:
            # Problems cleared — resolve PagerDuty incident
            resolve_pagerduty(args.pagerduty_routing_key, args.metrics_url)
            was_alerting = False

        prev_metrics = metrics

        if args.interval <= 0:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
