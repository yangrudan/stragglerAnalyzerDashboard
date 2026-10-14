#!/usr/bin/env python3
"""Generate an offline visual dashboard from straggler-analyzer logs."""

from __future__ import annotations

import argparse
import html
import json
import re
import shlex
import statistics
import sys
import webbrowser
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator


RETRY_RE = re.compile(
    r"dingtalk_notification_retrying .*?job_id=(?P<job>\S+) "
    r"iteration_id=(?P<iteration>-?\d+) .*?failed_round=(?P<round>\d+) "
    r"retry_in_ms=(?P<delay>\d+) reason=(?P<reason>.*)$"
)
RECOVERED_RE = re.compile(
    r"dingtalk_notification_sent_after_retry .*?job_id=(?P<job>\S+) "
    r"iteration_id=(?P<iteration>-?\d+) .*?failed_rounds=(?P<rounds>\d+)"
)


def median(values: Iterable[float]) -> float | None:
    items = [float(value) for value in values if value is not None]
    return statistics.median(items) if items else None


def parse_value(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"none", "null", "disabled"}:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def parse_key_values(line: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    try:
        tokens = shlex.split(line)
    except ValueError:
        tokens = line.split()
    for token in tokens:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if key and value:
            values[key] = parse_value(value)
    return values


def iter_jsonl(path: Path) -> Iterator[dict[str, Any] | None]:
    """Yield valid objects or None for malformed nonempty lines."""
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                yield None
                continue
            yield value if isinstance(value, dict) else None


def parse_jsonl(path: Path) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    malformed = 0
    for value in iter_jsonl(path):
        if value is None:
            malformed += 1
        else:
            records.append(value)
    return records, malformed


def count_jsonl(path: Path) -> tuple[int, int]:
    """Count suppressed events without retaining their payloads."""
    count = malformed = 0
    for value in iter_jsonl(path):
        if value is None:
            malformed += 1
        else:
            count += 1
    return count, malformed


def parse_log(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "source": str(path.resolve()),
        "config": {},
        "iterations": [],
        "slowdown_alerts": [],
        "node_alerts": [],
        "stats_samples": 0,
        "last_stats": {},
        "dingtalk_events": [],
        "other_events": Counter(),
        "malformed_json_lines": 0,
        "line_count": 0,
    }

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, 1):
            result["line_count"] = line_number
            text = line.strip()
            if not text:
                continue

            if text.startswith("{"):
                try:
                    record = json.loads(text)
                except json.JSONDecodeError:
                    result["malformed_json_lines"] += 1
                    continue
                if not isinstance(record, dict):
                    result["malformed_json_lines"] += 1
                    continue
                record["_line"] = line_number
                event = record.get("event")
                if event == "straggler_iteration_timing":
                    result["iterations"].append(record)
                elif event == "straggler_iteration_slowdown":
                    result["slowdown_alerts"].append(record)
                elif event == "straggler_analyzer_stats":
                    result["stats_samples"] += 1
                    result["last_stats"] = record
                elif record.get("alert_type"):
                    result["node_alerts"].append(record)
                elif event:
                    result["other_events"][str(event)] += 1
                continue

            if (
                text.startswith("straggler-analyzer ")
                and (
                    " bind=" in text
                    or "dingtalk_enabled=" in text
                    and "queue_capacity=" in text
                )
            ):
                result["config"].update(parse_key_values(text))

            retry = RETRY_RE.search(text)
            if retry:
                result["dingtalk_events"].append(
                    {
                        "kind": "retry",
                        "line": line_number,
                        "job_id": retry.group("job"),
                        "iteration_id": int(retry.group("iteration")),
                        "failed_round": int(retry.group("round")),
                        "retry_in_ms": int(retry.group("delay")),
                        "reason": retry.group("reason"),
                    }
                )
                continue
            recovered = RECOVERED_RE.search(text)
            if recovered:
                result["dingtalk_events"].append(
                    {
                        "kind": "recovered",
                        "line": line_number,
                        "job_id": recovered.group("job"),
                        "iteration_id": int(recovered.group("iteration")),
                        "failed_rounds": int(recovered.group("rounds")),
                    }
                )

    result["iterations"].sort(key=lambda item: item.get("iteration_id", -1))
    result["slowdown_alerts"].sort(key=lambda item: item.get("iteration_id", -1))
    result["node_alerts"].sort(
        key=lambda item: (item.get("iteration_id", -1), item.get("global_rank", -1))
    )
    result["other_events"] = dict(result["other_events"])
    return result


def group_slow_episodes(
    iterations: list[dict[str, Any]], alerts: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    alert_by_iteration = {
        int(alert["iteration_id"]): alert
        for alert in alerts
        if isinstance(alert.get("iteration_id"), int)
    }
    episodes: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []

    def finish() -> None:
        if not current:
            return
        ids = [int(item["iteration_id"]) for item in current]
        actual = [float(item.get("actual_iteration_ms", 0.0)) for item in current]
        episode_alerts = [
            alert_by_iteration[item_id]
            for item_id in ids
            if item_id in alert_by_iteration
        ]
        top_ranks: Counter[int] = Counter()
        for alert in episode_alerts:
            candidates = alert.get("candidates") or []
            if candidates:
                rank = candidates[0].get("global_rank")
                if isinstance(rank, int):
                    top_ranks[rank] += 1
        episodes.append(
            {
                "start": ids[0],
                "end": ids[-1],
                "count": len(ids),
                "min_ms": min(actual),
                "max_ms": max(actual),
                "first_alert": (
                    int(episode_alerts[0]["iteration_id"]) if episode_alerts else None
                ),
                "alert_count": len(episode_alerts),
                "dominant_rank": top_ranks.most_common(1)[0][0] if top_ranks else None,
            }
        )
        current.clear()

    previous_id: int | None = None
    for item in iterations:
        iteration_id = item.get("iteration_id")
        if not isinstance(iteration_id, int):
            continue
        is_slow = bool(item.get("slow"))
        if is_slow and (previous_id is None or iteration_id == previous_id + 1):
            current.append(item)
        elif is_slow:
            finish()
            current.append(item)
        else:
            finish()
        previous_id = iteration_id
    finish()
    return episodes


def aggregate_ranks(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for alert in alerts:
        for candidate in alert.get("candidates") or []:
            peer = candidate.get("pair_peer") or {}
            key = (
                candidate.get("global_rank"),
                candidate.get("hostname") or "unknown",
                candidate.get("host_ip") or "unknown",
                peer.get("global_rank"),
            )
            entry = grouped.setdefault(
                key,
                {
                    "global_rank": key[0],
                    "hostname": key[1],
                    "host_ip": key[2],
                    "peer_rank": key[3],
                    "count": 0,
                    "high_count": 0,
                    "impact_count": 0,
                    "max_ratio": 0.0,
                    "max_explanation": 0.0,
                    "regions": Counter(),
                },
            )
            entry["count"] += 1
            entry["high_count"] += int(bool(candidate.get("high_confidence")))
            entry["impact_count"] += int(bool(candidate.get("impact_confidence")))
            entry["max_ratio"] = max(
                entry["max_ratio"], float(candidate.get("ratio") or 0.0)
            )
            entry["max_explanation"] = max(
                entry["max_explanation"],
                float(candidate.get("explanation_ratio") or 0.0),
            )
            entry["regions"][candidate.get("region") or "unknown"] += 1

    rows = list(grouped.values())
    for row in rows:
        row["regions"] = [
            {"name": name, "count": count}
            for name, count in row["regions"].most_common()
        ]
    rows.sort(
        key=lambda row: (
            row["impact_count"],
            row["high_count"],
            row["count"],
            row["max_explanation"],
        ),
        reverse=True,
    )
    return rows


def build_model(
    parsed: dict[str, Any], suppressed_records: list[dict[str, Any]] | int | None = None
) -> dict[str, Any]:
    iterations = parsed["iterations"]
    alerts = parsed["slowdown_alerts"]
    episodes = group_slow_episodes(iterations, alerts)
    ranks = aggregate_ranks(alerts)
    last_stats = parsed.get("last_stats") or {}
    iteration_stats = last_stats.get("iteration_gate") or {}
    dingtalk_stats = last_stats.get("dingtalk") or {}

    expected_ms = median(
        item.get("expected_iteration_ms") for item in iterations
    )
    threshold_ratio = median(item.get("overrun_threshold") for item in iterations)
    threshold_ms = (
        expected_ms * (1.0 + threshold_ratio)
        if expected_ms is not None and threshold_ratio is not None
        else None
    )
    actual_values = (
        float(item["actual_iteration_ms"])
        for item in iterations
        if item.get("actual_iteration_ms") is not None
    )
    min_actual = max_actual = None
    for value in actual_values:
        min_actual = value if min_actual is None else min(min_actual, value)
        max_actual = value if max_actual is None else max(max_actual, value)
    normal_count = sum(not bool(item.get("slow")) for item in iterations)
    slow_count = sum(bool(item.get("slow")) for item in iterations)

    regions = []
    for name, stats in (last_stats.get("regions") or {}).items():
        regions.append(
            {
                "name": name,
                "records": int(stats.get("records_received") or 0),
                "alerts": int(stats.get("alerts_emitted") or 0),
                "suppressed_active": int(
                    stats.get("alerts_suppressed_active_episode") or 0
                ),
                "below_ratio": int(stats.get("decisions_ratio_below_threshold") or 0),
            }
        )
    regions.sort(key=lambda row: (row["alerts"], row["records"]), reverse=True)

    suppressed = suppressed_records if isinstance(suppressed_records, list) else []
    suppressed_count = (
        suppressed_records if isinstance(suppressed_records, int) else len(suppressed)
    )
    model = {
        **parsed,
        "other_events": parsed.get("other_events") or {},
        "expected_ms": expected_ms,
        "threshold_ratio": threshold_ratio,
        "threshold_ms": threshold_ms,
        "normal_count": normal_count,
        "slow_count": slow_count,
        "min_actual_ms": min_actual,
        "max_actual_ms": max_actual,
        "episodes": episodes,
        "rank_summary": ranks,
        "regions": regions,
        "iteration_stats": iteration_stats,
        "dingtalk_stats": dingtalk_stats,
        "suppressed_records": suppressed,
        "suppressed_count": suppressed_count,
    }
    model["insights"] = build_insights(model)
    model["generated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    return model


def format_ms(value: float | None) -> str:
    if value is None:
        return "未知"
    if value >= 1000:
        return f"{value / 1000:.2f}s"
    return f"{value:.1f}ms"


def build_insights(model: dict[str, Any]) -> list[str]:
    insights: list[str] = []
    expected = model.get("expected_ms")
    threshold = model.get("threshold_ms")
    ratio = model.get("threshold_ratio")
    if expected is not None and threshold is not None and ratio is not None:
        insights.append(
            f"迭代基准约 {format_ms(expected)}，慢阈值为 +{ratio * 100:.0f}%"
            f"（约 {format_ms(threshold)}）。"
        )
    insights.append(
        f"共解析 {len(model['iterations'])} 条迭代计时："
        f"正常 {model['normal_count']}，慢迭代 {model['slow_count']}，"
        f"形成 {len(model['episodes'])} 段连续慢区间。"
    )
    for index, episode in enumerate(model["episodes"], 1):
        text = (
            f"区间 {index}：iteration {episode['start']}–{episode['end']}，"
            f"共 {episode['count']} 轮，耗时 "
            f"{format_ms(episode['min_ms'])}–{format_ms(episode['max_ms'])}"
        )
        if episode["first_alert"] is None:
            text += "；未达到正式告警条件。"
        else:
            text += f"；首次正式告警在 iteration {episode['first_alert']}"
            if episode["dominant_rank"] is not None:
                text += f"，主要受影响 rank={episode['dominant_rank']}"
            text += "。"
        insights.append(text)
    if model["rank_summary"]:
        top = model["rank_summary"][0]
        insights.append(
            f"出现最频繁的高关联受影响项为 global rank={top['global_rank']}，"
            f"节点 {top['hostname']} / {top['host_ip']}，peer={top['peer_rank']}；"
            "同步耦合 region 只能说明受影响或等待关系，不能单独证明故障源。"
        )
    ding = model["dingtalk_stats"]
    if ding:
        insights.append(
            f"钉钉累计入队 {ding.get('enqueued', 0)}、最终发送 {ding.get('sent', 0)}、"
            f"失败轮次 {ding.get('failed', 0)}、重试后恢复 {ding.get('recovered_after_retry', 0)}、"
            f"当前 pending {ding.get('pending_delivery', 0)}。"
        )
    if model["dingtalk_events"]:
        recovered = [
            event for event in model["dingtalk_events"] if event["kind"] == "recovered"
        ]
        if recovered:
            values = ", ".join(
                f"iteration {event['iteration_id']}（{event['failed_rounds']} 轮失败后成功）"
                for event in recovered
            )
            insights.append(f"可靠重试已实际生效：{values}。")
    if model["suppressed_count"]:
        insights.append(
            f"额外载入 {model['suppressed_count']} 条整体迭代未超阈值的 hook 异常。"
        )
    elif model.get("iteration_stats"):
        recorded = model["iteration_stats"].get("hook_anomalies_recorded", 0)
        failed = model["iteration_stats"].get("hook_anomalies_write_failed", 0)
        insights.append(
            f"suppressed hook anomaly 统计：已记录 {recorded}，写入失败 {failed}。"
        )
    if model["malformed_json_lines"]:
        insights.append(
            f"发现 {model['malformed_json_lines']} 行以 JSON 开头但无法解析，请核对日志完整性。"
        )
    return insights


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def render_episode_cards(model: dict[str, Any]) -> str:
    cards = []
    for index, episode in enumerate(model["episodes"], 1):
        alert = (
            f"iteration {episode['first_alert']}"
            if episode["first_alert"] is not None
            else "未告警"
        )
        rank = (
            f"rank {episode['dominant_rank']}"
            if episode["dominant_rank"] is not None
            else "无定位"
        )
        cards.append(
            '<article class="episode">'
            f'<div class="episode-index">区间 {index}</div>'
            f'<h3>Iter {episode["start"]}–{episode["end"]}</h3>'
            f'<p>{episode["count"]} 轮 · {format_ms(episode["min_ms"])}–'
            f'{format_ms(episode["max_ms"])}</p>'
            f'<p class="muted">{esc(alert)} · {esc(rank)}</p>'
            "</article>"
        )
    return "".join(cards) or '<p class="empty">没有检测到连续慢迭代区间。</p>'


def render_rank_rows(model: dict[str, Any]) -> str:
    rows = []
    maximum = max(
        (row["impact_count"] for row in model["rank_summary"]), default=1
    )
    for row in model["rank_summary"][:12]:
        width = max(3.0, row["impact_count"] / maximum * 100.0)
        regions = ", ".join(item["name"] for item in row["regions"][:2])
        rows.append(
            '<div class="rank-row">'
            '<div class="rank-name">'
            f'<strong>global {esc(row["global_rank"])}</strong>'
            f'<span>{esc(row["hostname"])} · {esc(row["host_ip"])}</span>'
            f'<span>peer {esc(row["peer_rank"])} · {esc(regions)}</span>'
            "</div>"
            '<div class="rank-metric">'
            f'<div class="bar"><i style="width:{width:.1f}%"></i></div>'
            f'<span>impact {row["impact_count"]}/{row["count"]} · '
            f'最大解释度 {row["max_explanation"] * 100:.1f}% · '
            f'ratio {row["max_ratio"]:.3f}</span>'
            "</div></div>"
        )
    return "".join(rows) or '<p class="empty">没有正式 slowdown 候选。</p>'


def render_region_rows(model: dict[str, Any]) -> str:
    rows = []
    maximum = max((row["alerts"] for row in model["regions"]), default=1)
    for row in model["regions"]:
        width = max(0.0, row["alerts"] / maximum * 100.0) if maximum else 0.0
        rows.append(
            '<div class="region-row">'
            f'<span class="region-name">{esc(row["name"])}</span>'
            '<div class="region-bar">'
            f'<i style="width:{width:.1f}%"></i></div>'
            f'<span>{row["alerts"]} alerts / {row["records"]} records</span>'
            f'<span class="muted">{row["suppressed_active"]} active-episode suppressed</span>'
            "</div>"
        )
    return "".join(rows) or '<p class="empty">日志中没有 region 统计快照。</p>'


def render_alert_details(model: dict[str, Any]) -> str:
    blocks = []
    alerts = model["slowdown_alerts"]
    for alert in alerts:
        candidates = []
        for index, candidate in enumerate(alert.get("candidates") or [], 1):
            peer = candidate.get("pair_peer") or {}
            candidates.append(
                "<tr>"
                f"<td>{index}</td>"
                f'<td>{esc(candidate.get("global_rank", "-"))}</td>'
                f'<td>{esc(candidate.get("hostname", "-"))}<br>'
                f'<small>{esc(candidate.get("host_ip", "-"))}</small></td>'
                f'<td>{esc(peer.get("global_rank", "-"))}</td>'
                f'<td>{esc(candidate.get("region", "-"))}</td>'
                f'<td>{float(candidate.get("ratio") or 0):.3f}</td>'
                f'<td>{float(candidate.get("explanation_ratio") or 0) * 100:.1f}%</td>'
                f'<td>{esc(candidate.get("bad_windows", 0))}</td>'
                f'<td>{esc(bool(candidate.get("impact_confidence")))}</td>'
                "</tr>"
            )
        blocks.append(
            "<details>"
            "<summary>"
            f'Iteration {alert.get("iteration_id")} · '
            f'{format_ms(float(alert.get("actual_iteration_ms") or 0))} · '
            f'超出 {float(alert.get("overrun_ratio") or 0) * 100:.1f}% · '
            f'{len(alert.get("candidates") or [])} candidates'
            "</summary>"
            '<div class="table-wrap"><table><thead><tr>'
            "<th>#</th><th>受影响 rank</th><th>节点 / IP</th><th>peer</th>"
            "<th>region</th><th>ratio</th><th>解释度</th><th>坏窗口</th>"
            "<th>impact</th></tr></thead><tbody>"
            + "".join(candidates)
            + "</tbody></table></div></details>"
        )
    return "".join(blocks) or '<p class="empty">没有正式慢迭代告警。</p>'


def render_delivery(model: dict[str, Any]) -> str:
    ding = model["dingtalk_stats"]
    cards = [
        ("入队", ding.get("enqueued", 0)),
        ("已发送", ding.get("sent", 0)),
        ("失败轮次", ding.get("failed", 0)),
        ("重试恢复", ding.get("recovered_after_retry", 0)),
        ("冷却抑制", ding.get("suppressed_cooldown", 0)),
        ("Pending", ding.get("pending_delivery", 0)),
        ("队列丢弃", ding.get("dropped_queue_full", 0)),
    ]
    events = []
    for event in model["dingtalk_events"]:
        if event["kind"] == "retry":
            text = (
                f"Iteration {event['iteration_id']} · 第 {event['failed_round']} 轮失败 · "
                f"{event['retry_in_ms'] / 1000:g}s 后重试 · {event['reason']}"
            )
            tone = "warn"
        else:
            text = (
                f"Iteration {event['iteration_id']} · "
                f"{event['failed_rounds']} 轮失败后发送成功"
            )
            tone = "ok"
        events.append(f'<li class="{tone}">{esc(text)}</li>')
    return (
        '<div class="mini-cards">'
        + "".join(
            f'<div><strong>{esc(value)}</strong><span>{esc(label)}</span></div>'
            for label, value in cards
        )
        + "</div>"
        + (
            '<ol class="delivery-events">' + "".join(events) + "</ol>"
            if events
            else '<p class="empty">没有输出重试明细；即时成功只体现在累计统计中。</p>'
        )
    )


def sample_iterations(
    iterations: list[dict[str, Any]], alerts: list[dict[str, Any]], budget: int
) -> list[dict[str, Any]]:
    """Keep bucket extrema, endpoints, and every slow/alert iteration.

    The budget limits ordinary trend points; mandatory points may exceed it.
    Zero disables sampling.
    """
    if budget != 0 and budget < 4:
        raise ValueError("chart_points must be zero or at least 4")
    if budget == 0 or len(iterations) <= budget:
        return iterations
    alert_ids = {item.get("iteration_id") for item in alerts}
    selected = {0, len(iterations) - 1}
    for index, item in enumerate(iterations):
        if item.get("slow") or item.get("iteration_id") in alert_ids:
            selected.add(index)
    buckets = max(1, (budget - 2) // 2)
    def actual(index: int) -> float:
        return float(iterations[index].get("actual_iteration_ms") or 0)

    for bucket in range(buckets):
        start = bucket * len(iterations) // buckets
        end = (bucket + 1) * len(iterations) // buckets
        indices = range(start, end)
        selected.add(min(indices, key=actual))
        selected.add(max(indices, key=actual))
    return [iterations[index] for index in sorted(selected)]


def render_html(model: dict[str, Any], title: str, chart_points: int = 2000) -> str:
    chart_rows = sample_iterations(
        model["iterations"], model["slowdown_alerts"], chart_points
    )
    data = {
        "iterations": [
            {
                "id": item.get("iteration_id"),
                "actual": item.get("actual_iteration_ms"),
                "expected": item.get("expected_iteration_ms"),
                "threshold": (
                    float(item.get("expected_iteration_ms") or 0)
                    * (1 + float(item.get("overrun_threshold") or 0))
                ),
                "slow": bool(item.get("slow")),
                "streak": item.get("consecutive_slow_iterations"),
            }
            for item in chart_rows
        ],
        "alertIterations": [
            item.get("iteration_id") for item in model["slowdown_alerts"]
        ],
    }
    serialized = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    serialized = serialized.replace("<", "\\u003c").replace("</", "<\\/")
    cards = [
        ("迭代基准", format_ms(model["expected_ms"])),
        (
            "慢阈值",
            (
                f"+{model['threshold_ratio'] * 100:.0f}% / "
                f"{format_ms(model['threshold_ms'])}"
                if model["threshold_ratio"] is not None
                else "未知"
            ),
        ),
        ("计时记录", len(model["iterations"])),
        ("慢迭代", model["slow_count"]),
        ("正式告警", len(model["slowdown_alerts"])),
        ("慢区间", len(model["episodes"])),
    ]
    insights = "".join(f"<li>{esc(item)}</li>" for item in model["insights"])
    summary_cards = "".join(
        f'<div class="metric"><span>{esc(label)}</span><strong>{esc(value)}</strong></div>'
        for label, value in cards
    )

    template = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>
<style>
:root{--bg:#f4f6f8;--panel:#fff;--ink:#18202a;--muted:#687584;--line:#dfe5eb;--blue:#1769e0;--blue2:#8bb8ff;--red:#d13b45;--amber:#c57a00;--green:#18864b;--shadow:0 10px 28px rgba(22,34,48,.07)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.55 Inter,-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
header{padding:38px max(24px,calc((100vw - 1240px)/2));background:linear-gradient(125deg,#12243d,#183f74);color:#fff}
header h1{margin:0 0 8px;font-size:30px;letter-spacing:-.5px}header p{margin:3px 0;color:#cad9eb;word-break:break-all}
main{max-width:1240px;margin:0 auto;padding:24px}.metrics,.mini-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(135px,1fr));gap:12px}
.metric,.mini-cards>div{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:15px 16px;box-shadow:var(--shadow)}
.metric span,.mini-cards span{display:block;color:var(--muted);font-size:12px}.metric strong,.mini-cards strong{display:block;margin-top:5px;font-size:22px}
section{margin-top:20px;background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:22px;box-shadow:var(--shadow)}
h2{font-size:18px;margin:0 0 15px}h3{margin:5px 0;font-size:17px}.insights{margin:0;padding-left:22px}.insights li{margin:7px 0}
.chart-wrap{overflow-x:auto}.chart{width:100%;min-width:760px;height:auto;display:block}.axis{stroke:#acb7c2;stroke-width:1}.grid{stroke:#e8edf2;stroke-width:1}.series{fill:none;stroke:var(--blue);stroke-width:2}.expected{stroke:#788797;stroke-dasharray:5 5}.threshold{stroke:var(--amber);stroke-dasharray:7 5}.alert-line{stroke:rgba(209,59,69,.35);stroke-width:1}.point{fill:var(--blue)}.point.slow{fill:var(--red)}.chart text{fill:var(--muted);font-size:11px}
.legend{display:flex;gap:18px;flex-wrap:wrap;color:var(--muted);margin-top:8px}.legend i{display:inline-block;width:18px;height:3px;margin:0 6px 3px 0;background:var(--blue)}.legend .slow{background:var(--red)}.legend .limit{background:var(--amber)}
.episodes{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}.episode{border:1px solid var(--line);border-radius:12px;padding:15px;background:#fbfcfe}.episode p{margin:3px 0}.episode-index{font-size:12px;color:var(--red);font-weight:700}
.rank-row{display:grid;grid-template-columns:minmax(250px,1fr) 2fr;gap:18px;padding:13px 0;border-top:1px solid var(--line)}.rank-row:first-child{border-top:0}.rank-name span{display:block;color:var(--muted);font-size:12px}.bar,.region-bar{height:9px;background:#edf1f5;border-radius:20px;overflow:hidden;margin:5px 0}.bar i,.region-bar i{display:block;height:100%;background:linear-gradient(90deg,var(--blue),var(--blue2));border-radius:20px}
.rank-metric>span{font-size:12px;color:var(--muted)}.region-row{display:grid;grid-template-columns:minmax(180px,1.1fr) 2fr 1fr 1.2fr;gap:12px;align-items:center;padding:10px 0;border-top:1px solid var(--line)}.region-row:first-child{border-top:0}.region-name{font-weight:650}
details{border-top:1px solid var(--line);padding:12px 0}details:first-child{border-top:0}summary{cursor:pointer;font-weight:650}.table-wrap{overflow-x:auto;margin-top:12px}table{border-collapse:collapse;width:100%;font-size:12px}th,td{text-align:left;border-bottom:1px solid var(--line);padding:8px 9px;white-space:nowrap}th{color:var(--muted);background:#f7f9fb}
.delivery-events{padding-left:24px}.delivery-events li{margin:8px 0}.delivery-events .warn::marker{color:var(--amber)}.delivery-events .ok::marker{color:var(--green)}.muted,.empty{color:var(--muted)}
footer{max-width:1240px;margin:0 auto;padding:10px 24px 30px;color:var(--muted);font-size:12px}
@media(max-width:760px){header{padding:26px 18px}header h1{font-size:24px}main{padding:14px}section{padding:16px}.rank-row{grid-template-columns:1fr}.region-row{grid-template-columns:1fr}.region-bar{width:100%}}
</style>
</head>
<body>
<header><h1>__TITLE__</h1><p>来源：__SOURCE__</p><p>生成时间：__GENERATED__ · __LINES__ lines · __STATS__ stats snapshots</p></header>
<main>
<div class="metrics">__SUMMARY_CARDS__</div>
<section><h2>自动结论</h2><ol class="insights">__INSIGHTS__</ol></section>
<section><h2>迭代耗时趋势</h2><p class="muted">__CHART_NOTE__</p><div class="chart-wrap"><svg id="iteration-chart" class="chart" viewBox="0 0 1120 390" role="img" aria-label="迭代耗时趋势图"></svg></div><div class="legend"><span><i></i>实际耗时</span><span><i class="slow"></i>慢迭代</span><span><i class="limit"></i>慢阈值</span></div></section>
<section><h2>连续慢迭代区间</h2><div class="episodes">__EPISODES__</div></section>
<section><h2>受影响 rank 聚合</h2>__RANK_ROWS__</section>
<section><h2>Region 观测</h2>__REGION_ROWS__</section>
<section><h2>正式 slowdown 明细</h2>__ALERT_DETAILS__</section>
<section><h2>钉钉发送与可靠重试</h2>__DELIVERY__</section>
</main>
<footer>本报告完全离线生成。受影响 rank 尤其是 synchronization-coupled region 中的等待方，不等同于已证明的故障源。</footer>
<script>
const DATA=__DATA__;
(function(){
  const svg=document.getElementById("iteration-chart");
  const ns="http://www.w3.org/2000/svg", W=1120,H=390,m={l:66,r:24,t:22,b:45};
  const rows=DATA.iterations;
  if(!rows.length){svg.innerHTML='<text x="50%" y="50%" text-anchor="middle">没有迭代计时数据</text>';return;}
  let xmin=Infinity,xmax=-Infinity,ymax=0;
  for(const row of rows){xmin=Math.min(xmin,row.id);xmax=Math.max(xmax,row.id);ymax=Math.max(ymax,row.actual,row.threshold||0);}
  ymax*=1.08;
  const x=v=>m.l+(v-xmin)/(Math.max(1,xmax-xmin))*(W-m.l-m.r);
  const y=v=>H-m.b-v/Math.max(1,ymax)*(H-m.t-m.b);
  const add=(name,attrs,text)=>{const el=document.createElementNS(ns,name);Object.entries(attrs||{}).forEach(([k,v])=>el.setAttribute(k,v));if(text!==undefined)el.textContent=text;svg.appendChild(el);return el;};
  for(let i=0;i<=5;i++){const value=ymax*i/5,yy=y(value);add("line",{x1:m.l,y1:yy,x2:W-m.r,y2:yy,class:"grid"});add("text",{x:m.l-8,y:yy+4,"text-anchor":"end"},value>=1000?(value/1000).toFixed(1)+"s":Math.round(value)+"ms");}
  add("line",{x1:m.l,y1:H-m.b,x2:W-m.r,y2:H-m.b,class:"axis"});
  add("line",{x1:m.l,y1:m.t,x2:m.l,y2:H-m.b,class:"axis"});
  const ticks=Math.min(8,Math.max(2,rows.length-1));for(let i=0;i<=ticks;i++){const value=Math.round(xmin+(xmax-xmin)*i/ticks);add("text",{x:x(value),y:H-18,"text-anchor":"middle"},"iter "+value);}
  const expected=rows.find(r=>r.expected)?.expected,limit=rows.find(r=>r.threshold)?.threshold;
  if(expected)add("line",{x1:m.l,y1:y(expected),x2:W-m.r,y2:y(expected),class:"expected"});
  if(limit)add("line",{x1:m.l,y1:y(limit),x2:W-m.r,y2:y(limit),class:"threshold"});
  new Set(DATA.alertIterations).forEach(id=>add("line",{x1:x(id),y1:m.t,x2:x(id),y2:H-m.b,class:"alert-line"}));
  add("polyline",{points:rows.map(r=>x(r.id)+","+y(r.actual)).join(" "),class:"series"});
  rows.forEach(r=>{const c=add("circle",{cx:x(r.id),cy:y(r.actual),r:r.slow?3.5:2.2,class:r.slow?"point slow":"point"});const t=document.createElementNS(ns,"title");t.textContent="Iteration "+r.id+": "+r.actual.toFixed(1)+" ms, streak="+r.streak;c.appendChild(t);});
})();
</script>
</body></html>"""
    replacements = {
        "__CHART_NOTE__": f"趋势图显示 {len(chart_rows)} / {len(model['iterations'])} 条记录；统计基于全量数据。降采样保留首尾、分桶极值、全部慢点和告警位置。",
        "__TITLE__": esc(title),
        "__SOURCE__": esc(model["source"]),
        "__GENERATED__": esc(model["generated_at"]),
        "__LINES__": esc(model["line_count"]),
        "__STATS__": esc(model["stats_samples"]),
        "__SUMMARY_CARDS__": summary_cards,
        "__INSIGHTS__": insights,
        "__EPISODES__": render_episode_cards(model),
        "__RANK_ROWS__": render_rank_rows(model),
        "__REGION_ROWS__": render_region_rows(model),
        "__ALERT_DETAILS__": render_alert_details(model),
        "__DELIVERY__": render_delivery(model),
        "__DATA__": serialized,
    }
    for marker, value in replacements.items():
        template = template.replace(marker, value)
    return template


def default_output(log_path: Path) -> Path:
    return log_path.with_name(log_path.name + ".dashboard.html")


def load_suppressed(
    explicit_path: Path | None, parsed: dict[str, Any]
) -> tuple[int, Path | None, int]:
    candidate = explicit_path
    if candidate is None:
        configured = parsed.get("config", {}).get("suppressed_anomaly_log")
        if configured:
            configured_path = Path(str(configured))
            if configured_path.is_file():
                candidate = configured_path
    if candidate is None:
        return 0, None, 0
    if not candidate.is_file():
        raise FileNotFoundError(f"suppressed anomaly log does not exist: {candidate}")
    count, malformed = count_jsonl(candidate)
    return count, candidate, malformed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze a straggler-analyzer log and generate an offline HTML dashboard."
    )
    parser.add_argument("log", type=Path, help="path to analyzer.log")
    parser.add_argument(
        "-o", "--output", type=Path, help="output HTML path (default: LOG.dashboard.html)"
    )
    parser.add_argument(
        "--suppressed-log",
        type=Path,
        help="optional suppressed-hook-anomalies.jsonl to include",
    )
    parser.add_argument("--title", default="Straggler Analyzer Dashboard")
    parser.add_argument(
        "--chart-points", type=int, default=2000,
        help="ordinary trend point budget (default: 2000; 0 disables sampling); slow/alert points always retained",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="print the automatic conclusions without generating HTML",
    )
    parser.add_argument(
        "--open", action="store_true", help="open the generated report in a browser"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.chart_points != 0 and args.chart_points < 4:
        print("error: --chart-points must be zero or at least 4", file=sys.stderr)
        return 2
    if not args.log.is_file():
        print(f"error: log does not exist: {args.log}", file=sys.stderr)
        return 2
    parsed = parse_log(args.log)
    try:
        suppressed, suppressed_path, suppressed_malformed = load_suppressed(
            args.suppressed_log, parsed
        )
    except (OSError, FileNotFoundError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    parsed["suppressed_source"] = (
        str(suppressed_path.resolve()) if suppressed_path is not None else None
    )
    parsed["malformed_suppressed_lines"] = suppressed_malformed
    model = build_model(parsed, suppressed)
    for insight in model["insights"]:
        print(f"- {insight}")
    if args.summary_only:
        return 0
    output = args.output or default_output(args.log)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_html(model, args.title, args.chart_points), encoding="utf-8")
    print(f"dashboard: {output.resolve()}")
    if args.open:
        webbrowser.open(output.resolve().as_uri())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
