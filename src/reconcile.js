// 分布式评估结果对账器。
// 输入分片清单与各 worker 的上传摘要，输出最终加权最终指标或拒绝原因。
// 规则：
//   1. 同一 shard 的重复上传（重试/重复）只计一次；
//   2. 缺失 shard 时拒绝合并，绝不做静默平均；
//   3. 配置版本不一致的结果不可合并；
//   4. 指标按样本数加权，而不是按 worker 数平均；
//   5. 同一 shard 出现互相矛盾的结果时拒绝合并。

const DEFAULT_CONFIDENCE_LEVEL = 0.95;

// 标准正态分布分位数（常用置信水平查表，避免引入依赖）。
const Z_TABLE = new Map([
  [0.8, 1.2815515655446004],
  [0.9, 1.6448536269514722],
  [0.95, 1.959963984540054],
  [0.98, 2.3263478740408408],
  [0.99, 2.5758293035489004],
  [0.999, 3.2905267314919255],
]);

function zScore(level) {
  if (Z_TABLE.has(level)) return Z_TABLE.get(level);
  // 中心区域有理逼近，足够对账场景使用。
  const p = 1 - (1 - level) / 2;
  const a = -3.969683028665376e1;
  const b = 2.209460984245205e2;
  const c = -2.759285104469687e2;
  const d = 1.38357751867269e2;
  const e = -3.066479806614716e1;
  const f = 2.506628277459239;
  const q = p - 0.5;
  const r = q * q;
  return (((((a * r + b) * r + c) * r + d) * r + e) * r + f) * q;
}

// Wilson score interval：对聚合比例给出置信范围。
export function wilsonInterval(successes, total, level = DEFAULT_CONFIDENCE_LEVEL) {
  if (total <= 0) {
    return { level, lower: NaN, upper: NaN };
  }
  const z = zScore(level);
  const p = successes / total;
  const z2 = z * z;
  const denom = 1 + z2 / total;
  const center = p + z2 / (2 * total);
  const margin = z * Math.sqrt((p * (1 - p) + z2 / (4 * total)) / total);
  return {
    level,
    lower: Math.max(0, (center - margin) / denom),
    upper: Math.min(1, (center + margin) / denom),
  };
}

function countsKey(upload) {
  const entries = Object.entries(upload.counts ?? {}).sort(([a], [b]) =>
    a.localeCompare(b),
  );
  return JSON.stringify({
    configVersion: upload.configVersion,
    samples: upload.samples,
    counts: entries,
  });
}

function validateUpload(upload, index) {
  const problems = [];
  if (typeof upload.shardId !== 'string' || upload.shardId.length === 0) {
    problems.push(`uploads[${index}] 缺少 shardId`);
  }
  if (upload.status !== 'ok' && upload.status !== 'failed') {
    problems.push(`uploads[${index}] status 必须是 "ok" 或 "failed"`);
  }
  if (upload.status === 'ok') {
    if (typeof upload.configVersion !== 'string' || upload.configVersion.length === 0) {
      problems.push(`uploads[${index}] 缺少 configVersion`);
    }
    if (!Number.isInteger(upload.samples) || upload.samples <= 0) {
      problems.push(`uploads[${index}] samples 必须是正整数`);
    }
    if (upload.counts === null || typeof upload.counts !== 'object') {
      problems.push(`uploads[${index}] 缺少 counts`);
    } else {
      for (const [name, value] of Object.entries(upload.counts)) {
        if (!Number.isFinite(value) || value < 0) {
          problems.push(`uploads[${index}] counts.${name} 必须是非负数值`);
        }
        if (Number.isInteger(upload.samples) && value > upload.samples) {
          problems.push(`uploads[${index}] counts.${name} 超过样本数`);
        }
      }
    }
  }
  return problems;
}

/**
 * 对账入口。
 * @param {object} input
 * @param {Array<{shardId: string, start: number, end: number}>} input.expectedShards
 *   期望的分片清单，样本范围为左闭右开 [start, end)。
 * @param {Array<object>} input.uploads
 *   worker 上传摘要：{ shardId, workerId, attempt, status, configVersion, samples, counts }。
 * @param {number} [input.confidenceLevel] 置信水平，默认 0.95。
 * @returns {object} 对账报告。
 */
export function reconcile(input) {
  const { expectedShards = [], uploads = [] } = input ?? {};
  const confidenceLevel = input?.confidenceLevel ?? DEFAULT_CONFIDENCE_LEVEL;

  const report = {
    status: 'ok',
    validShards: [],
    metrics: {},
    duplicates: [],
    failedUploads: [],
    reasons: [],
  };

  const reject = (reason) => {
    report.status = 'rejected';
    report.reasons.push(reason);
  };

  const expectedById = new Map();
  for (const shard of expectedShards) {
    if (expectedById.has(shard.shardId)) {
      reject(`分片清单中 shardId 重复: ${shard.shardId}`);
      continue;
    }
    if (!Number.isInteger(shard.start) || !Number.isInteger(shard.end) || shard.end <= shard.start) {
      reject(`分片 ${shard.shardId} 的样本范围非法: [${shard.start}, ${shard.end})`);
      continue;
    }
    expectedById.set(shard.shardId, shard);
  }

  uploads.forEach((upload, index) => {
    for (const problem of validateUpload(upload, index)) reject(problem);
  });
  if (report.status === 'rejected') return finalize(report);

  const okByShard = new Map();
  for (const upload of uploads) {
    if (upload.status === 'failed') {
      report.failedUploads.push({
        shardId: upload.shardId,
        workerId: upload.workerId ?? null,
        attempt: upload.attempt ?? null,
      });
      continue;
    }
    if (!okByShard.has(upload.shardId)) okByShard.set(upload.shardId, []);
    okByShard.get(upload.shardId).push(upload);
  }

  // 未知 shard 的上传不可并入。
  for (const shardId of okByShard.keys()) {
    if (!expectedById.has(shardId)) {
      reject(`收到未知分片 ${shardId} 的上传，无法并入期望清单`);
    }
  }

  // 每个 shard 选出唯一结果：完全相同的重试/重复上传只计一次。
  const chosenByShard = new Map();
  for (const [shardId, shardUploads] of okByShard) {
    if (!expectedById.has(shardId)) continue;
    const seen = new Map();
    for (const upload of shardUploads) {
      const key = countsKey(upload);
      if (seen.has(key)) {
        report.duplicates.push({
          shardId,
          workerId: upload.workerId ?? null,
          attempt: upload.attempt ?? null,
          keptWorkerId: seen.get(key).workerId ?? null,
          keptAttempt: seen.get(key).attempt ?? null,
        });
      } else {
        seen.set(key, upload);
      }
    }
    const distinct = [...seen.values()];
    if (distinct.length > 1) {
      reject(
        `分片 ${shardId} 存在 ${distinct.length} 份互相矛盾的结果` +
          `（来自 ${distinct.map((u) => u.workerId ?? '未知worker').join(', ')}），拒绝合并`,
      );
      continue;
    }
    chosenByShard.set(shardId, distinct[0]);
  }

  // 缺失 shard：拒绝，绝不静默平均。
  const missing = [];
  for (const shardId of expectedById.keys()) {
    if (!chosenByShard.has(shardId)) missing.push(shardId);
  }
  if (missing.length > 0) {
    reject(`缺失分片结果: ${missing.join(', ')}，不允许对剩余分片静默平均`);
  }

  // 配置版本一致性。
  const versions = new Map();
  for (const [shardId, upload] of chosenByShard) {
    if (!versions.has(upload.configVersion)) versions.set(upload.configVersion, []);
    versions.get(upload.configVersion).push(shardId);
  }
  if (versions.size > 1) {
    const detail = [...versions.entries()]
      .map(([v, shards]) => `${v} -> [${shards.join(', ')}]`)
      .join('; ');
    reject(`配置版本不一致，不可合并: ${detail}`);
  }

  // 样本数必须与期望范围一致。
  for (const [shardId, upload] of chosenByShard) {
    const expected = expectedById.get(shardId);
    const expectedSamples = expected.end - expected.start;
    if (upload.samples !== expectedSamples) {
      reject(
        `分片 ${shardId} 样本数不符: 上传 ${upload.samples}，期望 ${expectedSamples}` +
          `（范围 [${expected.start}, ${expected.end})）`,
      );
    }
  }

  if (report.status === 'rejected') return finalize(report);

  // 汇总：按样本数加权。
  const metricNames = new Set();
  for (const upload of chosenByShard.values()) {
    for (const name of Object.keys(upload.counts)) metricNames.add(name);
  }
  const totals = new Map([...metricNames].map((name) => [name, 0]));
  let totalSamples = 0;
  for (const [shardId, upload] of chosenByShard) {
    for (const name of metricNames) {
      if (!(name in upload.counts)) {
        reject(`分片 ${shardId} 缺少指标计数 ${name}`);
      }
    }
    if (report.status === 'rejected') continue;
    totalSamples += upload.samples;
    for (const [name, sum] of totals) {
      totals.set(name, sum + upload.counts[name]);
    }
    report.validShards.push({
      shardId,
      workerId: upload.workerId ?? null,
      attempt: upload.attempt ?? null,
      samples: upload.samples,
    });
  }
  if (report.status === 'rejected') return finalize(report);

  report.validShards.sort((a, b) => a.shardId.localeCompare(b.shardId));
  const [configVersion] = versions.keys();
  report.configVersion = configVersion;
  report.totalSamples = totalSamples;
  for (const [name, sum] of [...totals.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    report.metrics[name] = {
      value: sum / totalSamples,
      numerator: sum,
      denominator: totalSamples,
      confidence: wilsonInterval(sum, totalSamples, confidenceLevel),
    };
  }
  return finalize(report);
}

function finalize(report) {
  if (report.status === 'rejected') {
    report.metrics = {};
  }
  return report;
}
