# Analyzer Dashboard

将 Straggler Observer 的 `analyzer.log` 转成可离线打开的单文件 HTML 报告，同时在终端输出简短中文结论。

[离线版本]

## 快速使用

```bash
cd /home/yang/135-data/data/a100-megatron/analyzer-dashboard
./analyzer_dashboard.py "/home/yang/Downloads/analyzer (2).log"
```

默认报告生成在日志旁边：

```text
/home/yang/Downloads/analyzer (2).log.dashboard.html
```

指定输出位置：

```bash
./analyzer_dashboard.py /path/to/analyzer.log \
  -o /tmp/analyzer-dashboard.html
```

只看终端文字结论，不生成网页：

```bash
./analyzer_dashboard.py /path/to/analyzer.log --summary-only
```

同时载入 suppressed hook anomaly 文件：

```bash
./analyzer_dashboard.py /path/to/analyzer.log \
  --suppressed-log /path/to/suppressed-hook-anomalies.jsonl
```

本机有桌面环境时可以生成后自动打开：

```bash
./analyzer_dashboard.py /path/to/analyzer.log --open
```

## 报告内容

- 自动学习或手动配置的迭代基准、overrun 阈值和计时总数；
- iteration time 曲线、慢阈值、慢点和正式告警位置；
- 连续慢迭代区间，以及是否达到正式告警条件；
- 受影响 rank、节点/IP、pair peer、解释度和置信度聚合；
- 各 hook region 的记录量、告警量和 active-episode 抑制量；
- 每一轮 slowdown 的 Top-K 候选明细；
- 钉钉入队、发送、冷却抑制、失败轮次和可靠重试恢复记录；
- 可选的 `suppressed-hook-anomalies.jsonl` 计数。

报告不引用 CDN 或外部 JavaScript，复制 HTML 文件到其他机器仍能查看。

## 测试

```bash
python3 -m unittest -v
```

要求 Python 3.10 或更高版本，无第三方 Python 依赖。
