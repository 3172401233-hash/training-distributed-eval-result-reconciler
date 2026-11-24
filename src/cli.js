#!/usr/bin/env node
// 用法: node src/cli.js <input.json>
// 从 JSON 文件读取分片清单与上传摘要，打印对账报告。
import { readFile } from 'node:fs/promises';
import { reconcile } from './reconcile.js';

const [, , inputPath] = process.argv;
if (!inputPath) {
  console.error('用法: node src/cli.js <input.json>');
  process.exit(2);
}

const input = JSON.parse(await readFile(inputPath, 'utf8'));
const report = reconcile(input);
console.log(JSON.stringify(report, null, 2));
process.exit(report.status === 'ok' ? 0 : 1);
