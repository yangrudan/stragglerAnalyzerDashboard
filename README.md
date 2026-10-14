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

## 大文件处理

日志按行读取。suppressed JSONL 采用在线计数，不保留完整记录；空行忽略，非对象或无效 JSON 计为格式错误。

趋势图默认以 2000 个点作为普通点采样预算，保留首尾、各桶耗时最小/最大值、全部慢点和告警位置。异常点较多时会超过预算。统计、慢区间和告警明细仍基于完整数据，报告会显示绘图记录数与原始记录数。

```bash
./analyzer_dashboard.py /path/to/analyzer.log --chart-points 1000
# 禁用趋势降采样
./analyzer_dashboard.py /path/to/analyzer.log --chart-points 0
```

`--chart-points` 必须为 0 或至少 4。主日志的迭代、告警和重试事件仍保留在内存中，HTML 仍整体生成；这版并非固定内存的端到端流式处理。`--summary-only` 仅跳过 HTML 输出，异常密集日志仍可能消耗较多内存。

### 实际日志验证

在当前机器用 `/usr/bin/time` 对比 main 与此版本（单次运行，缓存和系统负载会影响耗时），均成功生成报告：

| 输入文件大小 | main 耗时 / 峰值内存 | 此版本耗时 / 峰值内存 | HTML 大小变化 |
| --- | --- | --- | --- |
| 238 MB | 2.83 s / 22.9 MiB | 2.66 s / 22.8 MiB | 216,448 → 216,601 字节 |
| 919 MB | 10.67 s / 38.4 MiB | 10.34 s / 32.7 MiB | 921,089 → 226,037 字节 |

919 MB 输入的趋势点从 8,641 降到 2,005；238 MB 输入只有 1,954 个点，未降采样。对比确认统计卡片和全部慢点一致，告警位置一致。另以 10,000 条约 1 KB 的 suppressed 记录验证在线计数，Python 跟踪的峰值分配小于 1 MiB；用 Node 和最小 DOM 模拟执行 200,000 点图表脚本验证不再触发函数参数数量限制，未测量真实浏览器渲染性能。

这些输入虽然字节数大，但保留的迭代记录较少，结果不能代表数百万条有效迭代记录的容量。
