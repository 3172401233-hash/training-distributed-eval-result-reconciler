# training-distributed-eval-result-reconciler
对账分布式评估分片、重试和重复结果

> 注意：本仓库**不运行真实评估**。它只对已经产生的评估结果（各 worker 上传的
> 指标计数摘要）做对账与合并，不加载模型、不跑推理、不访问数据集。

## 背景

分布式评估在多个 worker 上按 shard 执行。实际运行中常见三类问题：

- worker 失败重试后，同一 shard 的结果被重复上传；
- 部分 shard 永久缺失（worker 反复失败）；
- 不同 shard 用了不同的配置版本。

如果对上传结果简单平均，重复结果会改变样本权重，缺失 shard 会被静默跳过，
不同配置的结果会被错误合并。本工具在对账阶段显式处理这些问题。

## 对账规则

1. **重复只计一次**：同一 shard 内容完全一致（配置版本 + 样本数 + 指标计数）
   的重试/重复上传只保留一份，其余记入 `duplicates`。
2. **缺失即拒绝**：任何期望 shard 没有可用结果时拒绝合并，绝不对剩余 shard
   静默平均。
3. **配置不可混**：成功上传的配置版本不一致时拒绝合并。
4. **按样本加权**：最终指标 = 各 shard 计数之和 / 样本总数，不按 worker 数平均。
5. **矛盾即拒绝**：同一 shard 出现计数不一致的多份结果时拒绝合并。

## 输入

```json
{
  "expectedShards": [{ "shardId": "shard-0", "start": 0, "end": 100 }],
  "uploads": [
    {
      "shardId": "shard-0",
      "workerId": "w1",
      "attempt": 1,
      "status": "ok",
      "configVersion": "cfg-v1",
      "samples": 100,
      "counts": { "correct": 80 }
    }
  ],
  "confidenceLevel": 0.95
}
```

- `expectedShards`：期望分片清单，样本范围左闭右开 `[start, end)`。
- `uploads`：worker 上传摘要；`status` 为 `ok` 或 `failed`，`counts` 为各指标
  的命中计数（分子），`samples` 为该 shard 的样本数（分母）。

## 输出

```json
{
  "status": "ok",
  "validShards": ["..."],
  "metrics": {
    "correct": {
      "value": 0.85,
      "numerator": 170,
      "denominator": 200,
      "confidence": { "level": 0.95, "lower": 0.7939, "upper": 0.8929 }
    }
  },
  "duplicates": ["..."],
  "failedUploads": ["..."],
  "reasons": [],
  "configVersion": "cfg-v1",
  "totalSamples": 200
}
```

- `status` 为 `rejected` 时 `metrics` 为空，不可合并原因在 `reasons` 中。
- 置信范围使用 Wilson score interval（默认 95%）。

## 使用

```bash
# 作为库
node -e "import('./src/reconcile.js').then(m => console.log(m.reconcile(input)))"

# 作为 CLI（退出码 0=合并成功，1=拒绝）
node src/cli.js input.json

# 测试（Node.js 24.16.0，无第三方依赖）
node --test
```
