import { test } from 'node:test';
import assert from 'node:assert/strict';
import { reconcile, wilsonInterval } from '../src/reconcile.js';

const expectedShards = [
  { shardId: 'shard-0', start: 0, end: 100 },
  { shardId: 'shard-1', start: 100, end: 250 },
  { shardId: 'shard-2', start: 250, end: 300 },
];

function okUpload(shardId, workerId, attempt, samples, correct, configVersion = 'cfg-v1') {
  return {
    shardId,
    workerId,
    attempt,
    status: 'ok',
    configVersion,
    samples,
    counts: { correct },
  };
}

test('正常路径：按样本数加权而非按 worker 数平均', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      okUpload('shard-1', 'w2', 1, 150, 135),
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  assert.equal(report.status, 'ok');
  assert.equal(report.totalSamples, 300);
  // 加权: (80+135+25)/300 = 0.8；若按 worker 简单平均则为 (0.8+0.9+0.5)/3 ≈ 0.7333。
  assert.equal(report.metrics.correct.value, 240 / 300);
  assert.equal(report.metrics.correct.numerator, 240);
  assert.equal(report.metrics.correct.denominator, 300);
  assert.notEqual(report.metrics.correct.value, (0.8 + 0.9 + 0.5) / 3);
  assert.deepEqual(
    report.validShards.map((s) => s.shardId),
    ['shard-0', 'shard-1', 'shard-2'],
  );
  assert.equal(report.configVersion, 'cfg-v1');
});

test('置信范围：Wilson 区间覆盖点估计且边界合法', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      okUpload('shard-1', 'w2', 1, 150, 135),
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  const ci = report.metrics.correct.confidence;
  assert.equal(ci.level, 0.95);
  assert.ok(ci.lower < report.metrics.correct.value);
  assert.ok(ci.upper > report.metrics.correct.value);
  assert.ok(ci.lower >= 0 && ci.upper <= 1);
  // 与已知 Wilson 结果对比（240/300, 95%）。
  assert.ok(Math.abs(ci.lower - 0.7511) < 1e-3);
  assert.ok(Math.abs(ci.upper - 0.8413) < 1e-3);
});

test('重试：同一 worker 重试上传相同结果只计一次', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      okUpload('shard-0', 'w1', 2, 100, 80),
      okUpload('shard-1', 'w2', 1, 150, 135),
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  assert.equal(report.status, 'ok');
  assert.equal(report.duplicates.length, 1);
  assert.equal(report.duplicates[0].shardId, 'shard-0');
  assert.equal(report.duplicates[0].attempt, 2);
  assert.equal(report.metrics.correct.numerator, 240);
  assert.equal(report.metrics.correct.denominator, 300);
});

test('重复上传：不同 worker 重复上传同一 shard 相同结果只计一次', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      okUpload('shard-0', 'w1-backup', 1, 100, 80),
      okUpload('shard-1', 'w2', 1, 150, 135),
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  assert.equal(report.status, 'ok');
  assert.equal(report.duplicates.length, 1);
  assert.equal(report.duplicates[0].workerId, 'w1-backup');
  assert.equal(report.duplicates[0].keptWorkerId, 'w1');
  assert.equal(report.metrics.correct.value, 0.8);
});

test('shard 缺失：拒绝合并而不是静默平均', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  assert.equal(report.status, 'rejected');
  assert.ok(report.reasons.some((r) => r.includes('shard-1') && r.includes('缺失')));
  assert.deepEqual(report.metrics, {});
});

test('shard 永久缺失：失败上传不能顶替缺失分片', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      { shardId: 'shard-1', workerId: 'w2', attempt: 3, status: 'failed' },
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  assert.equal(report.status, 'rejected');
  assert.equal(report.failedUploads.length, 1);
  assert.equal(report.failedUploads[0].shardId, 'shard-1');
  assert.ok(report.reasons.some((r) => r.includes('缺失')));
});

test('配置变化：不同 configVersion 的结果不可合并', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80, 'cfg-v1'),
      okUpload('shard-1', 'w2', 1, 150, 135, 'cfg-v2'),
      okUpload('shard-2', 'w3', 1, 50, 25, 'cfg-v1'),
    ],
  });
  assert.equal(report.status, 'rejected');
  assert.ok(report.reasons.some((r) => r.includes('配置版本不一致')));
  assert.ok(report.reasons.some((r) => r.includes('cfg-v1') && r.includes('cfg-v2')));
});

test('矛盾结果：同一 shard 不同计数的上传被拒绝', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      okUpload('shard-0', 'w1', 2, 100, 81),
      okUpload('shard-1', 'w2', 1, 150, 135),
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  assert.equal(report.status, 'rejected');
  assert.ok(report.reasons.some((r) => r.includes('shard-0') && r.includes('矛盾')));
});

test('样本数与期望范围不符时拒绝', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 99, 80),
      okUpload('shard-1', 'w2', 1, 150, 135),
      okUpload('shard-2', 'w3', 1, 50, 25),
    ],
  });
  assert.equal(report.status, 'rejected');
  assert.ok(report.reasons.some((r) => r.includes('样本数不符')));
});

test('未知分片的上传不可并入', () => {
  const report = reconcile({
    expectedShards,
    uploads: [
      okUpload('shard-0', 'w1', 1, 100, 80),
      okUpload('shard-1', 'w2', 1, 150, 135),
      okUpload('shard-2', 'w3', 1, 50, 25),
      okUpload('shard-9', 'w4', 1, 10, 9),
    ],
  });
  assert.equal(report.status, 'rejected');
  assert.ok(report.reasons.some((r) => r.includes('未知分片')));
});

test('wilsonInterval 边界：全部正确与零样本', () => {
  const perfect = wilsonInterval(100, 100);
  assert.equal(perfect.upper, 1);
  assert.ok(perfect.lower < 1 && perfect.lower > 0.9);
  const empty = wilsonInterval(0, 0);
  assert.ok(Number.isNaN(empty.lower));
});
