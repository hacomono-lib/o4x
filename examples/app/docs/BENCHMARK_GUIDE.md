# O4X Comprehensive Benchmark Implementation Guide

## 目的

本ガイドは、o4x の **dispatcher ボトルネック分析** を実測ベースで実施するための実装と実行手順を記載します。

**検証の中心テーマ**：
- Dispatcher の本当のボトルネックは何か（DB / Lock / Connection / Go runtime）
- Instance 数と Worker 数のどちらが効くのか
- 推測ではなく実測で示す

## 実装済みコンポーネント

### 1. Instrumentation Infrastructure

**目的**: SELECT FOR UPDATE SKIP LOCKED の詳細計測

**実装ファイル**:
- `internal/instrumentation/metrics.go` - メトリクス収集・集約
- `internal/instrumentation/repository.go` - Repository ラッパー

**計測内容**:
- SELECT 実行時間 (μs / ms 単位)
- 取得 row 数
- Requested limit との比較 (utilization %)
- Worker ごとの統計
- Empty fetch 率

**出力例**:
```json
{
  "level": "INFO",
  "msg": "fetch_operation",
  "worker_id": 0,
  "duration_ms": 2,
  "duration_us": 2145,
  "rows_returned": 10,
  "requested_limit": 10,
  "utilization_pct": 100
}
```

**集約統計**:
```json
{
  "level": "INFO",
  "msg": "fetch_stats_summary",
  "total_fetches": 150,
  "total_rows": 1200,
  "empty_fetches": 30,
  "empty_fetch_pct": 20,
  "avg_duration_ms": 1.8,
  "avg_duration_us": 1823,
  "avg_rows_per_fetch": 8
}
```

### 2. Instrumented Dispatcher

**ファイル**: `cmd/dispatcher-bench/main.go`

**特徴**:
- 標準 dispatcher と同一ロジック
- InstrumentedBatchRepository を使用
- 10 秒ごとに統計を自動出力
- シャットダウン時に最終統計を出力

**使用方法**:
```bash
# 標準起動 (worker=2)
go run cmd/dispatcher-bench/main.go

# Multi-queue モード
go run cmd/dispatcher-bench/main.go --multi-queue

# Worker 数指定
go run cmd/dispatcher-bench/main.go --workers 5
```

### 3. Matrix Benchmark Script

**ファイル**: `scripts/matrix-bench.sh`

**目的**: Dispatcher instances × Workers のマトリクステスト

**デフォルト設定**:
- Dispatcher instances: 1, 3, 5
- Workers per instance: 1, 5, 10
- 合計 9 パターン

**実行方法**:
```bash
# デフォルト (1000 messages, user event)
./scripts/matrix-bench.sh

# カスタム設定
./scripts/matrix-bench.sh 5000 notification
```

**出力**:
- `reports/matrix/{timestamp}_d{instances}_w{workers}.txt`
- 各テストの詳細レポート
- 最終サマリー（全パターン比較表）

## 未実装コンポーネント (次のステップ)

### 4. PostgreSQL Connection Metrics Collector

**目的**: DB 接続プールの詳細分析

**実装すべき内容**:
```go
// internal/instrumentation/pg_metrics.go
type PGMetricsCollector struct {
    // pg_stat_activity から取得
    TotalConnections  int
    ActiveConnections int
    IdleConnections   int
    WaitingConnections int

    // pgx pool stats から取得
    MaxConns          int
    AcquiredConns     int
    AvailableConns    int
    WaitDuration      time.Duration
}

func (c *PGMetricsCollector) CollectFromPool(pool *pgxpool.Pool) {
    stats := pool.Stat()
    // stats.AcquiredConns(), stats.IdleConns(), etc.
}

func (c *PGMetricsCollector) CollectFromPG(ctx context.Context, pool *pgxpool.Pool) {
    // SELECT count(*), state FROM pg_stat_activity GROUP BY state
}
```

**統合方法**:
- `dispatcher-bench/main.go` に統合
- 10 秒ごとに PG metrics も出力
- final report に含める

### 5. Notification Consumer Mock Patterns

**目的**: 遅い consumer のシミュレーション

**実装すべき handler**:
```go
// cmd/consumer/handlers/mock_handlers.go
type MockNotificationHandler struct {
    SleepMin time.Duration
    SleepMax time.Duration
}

func (h *MockNotificationHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // ランダムな sleep で外部 API 呼び出しをシミュレート
    sleep := h.SleepMin + time.Duration(rand.Int63n(int64(h.SleepMax-h.SleepMin)))
    time.Sleep(sleep)
    return nil
}
```

**テストパターン**:
- **Fast**: 50ms - 100ms (DB 更新相当)
- **Medium**: 250ms - 500ms (外部 API 軽量)
- **Slow**: 500ms - 2s (外部 API 重量)

**環境変数で制御**:
```yaml
# docker-compose.yml
consumer-notification:
  environment:
    MOCK_MODE: "true"
    MOCK_SLEEP_MIN: "250ms"
    MOCK_SLEEP_MAX: "2s"
```

### 6. Long-Running Benchmark Script

**目的**: 安定状態での throughput 計測

**実装すべき内容**:
```bash
#!/bin/bash
# scripts/long-bench.sh

MESSAGES=10000
WARMUP_MESSAGES=1000

# Warmup phase
send_messages $WARMUP_MESSAGES
wait_completion

# Clear metrics
reset_metrics

# Actual benchmark
BENCH_START=$(date +%s.%N)
send_messages $MESSAGES
wait_completion
BENCH_END=$(date +%s.%N)

# Calculate stable throughput
DURATION=$(echo "$BENCH_END - $BENCH_START" | bc)
THROUGHPUT=$(echo "$MESSAGES / $DURATION" | bc -l)
```

**ポイント**:
- Warmup フェーズで JIT / キャッシュを安定化
- 計測フェーズは warmup 後の安定区間のみ
- 最低 5,000 messages、推奨 10,000 messages

## 実行計画（推奨順序）

### Phase 1: 基本動作確認

```bash
# 1. Infrastructure を起動
cd examples/app
./scripts/start.sh

# 2. Instrumented dispatcher を起動
go run cmd/dispatcher-bench/main.go --workers 2

# 3. 別ターミナルで少量メッセージ送信
go run benchmark/main.go --endpoint http://localhost:18000 \
    --type user --requests 100 --concurrency 20

# 4. Logs で metrics を確認
# fetch_operation, fetch_stats_summary が出力されていることを確認
```

### Phase 2: Instance × Worker Matrix

**前提条件**:
- `docker-compose.yml` の dispatcher セクションに `WORKERS` 環境変数サポート追加

```yaml
dispatcher:
  environment:
    WORKERS: ${WORKERS:-2}  # Default to 2
  command: ["./dispatcher", "--workers", "${WORKERS}"]
```

**実行**:
```bash
# Matrix benchmark (9 パターン)
./scripts/matrix-bench.sh 1000 user

# 結果確認
cat reports/matrix/*_d*_w*.txt

# サマリー表示
grep "Throughput:" reports/matrix/*.txt
```

### Phase 3: Slow Consumer + Dispatcher Scaling

**実装後の実行フロー**:
```bash
# 1. Mock handler を実装
# 2. docker-compose.yml を更新
# 3. Slow consumer (250ms-2s) で再テスト

./scripts/matrix-bench.sh 1000 notification

# 期待結果:
# - Dispatcher scaling が consumer throughput に与える影響を観測
# - Slow consumer では dispatcher scaling の効果が大きいことを確認
```

### Phase 4: 長時間ベンチマーク

```bash
# 10,000 messages で安定 throughput を計測
./scripts/long-bench.sh 10000 user

# 各パターンで実行
for d in 1 3 5; do
    for w in 1 5 10; do
        ./scripts/long-bench.sh 10000 user $d $w
    done
done
```

### Phase 5: PostgreSQL Connection Analysis

**実装後の分析フロー**:
1. Matrix benchmark 実行中に PG metrics を収集
2. Connection pool の utilization を確認
3. Wait duration から bottleneck を特定

**期待される insights**:
- Worker 増加 → Connection pool 枯渇 → Wait duration 増加
- Instance 増加 → Total connections 増加 → PG 側の limit 到達
- Optimal configuration の決定

## 最終レポート構成（目標）

```markdown
# O4X Dispatcher Bottleneck Analysis Report

## 1. Executive Summary

**Scope and Prerequisites:**
- Test environment: LocalStack SQS, PostgreSQL, Docker (single host), GOMAXPROCS=1
- Workload: Dense (all messages in single outbox table)
- Constraint: No work partitioning implemented

**Key Finding:**
Without work partitioning, increasing dispatcher instances causes batch fragmentation, severely reducing throughput (d1: 728 msg/sec → d3: 67 msg/sec → d5: 37 msg/sec).

**What This Means:**
- This is NOT a general statement about horizontal scaling
- This is specific to dense workloads without work partitioning
- With work partitioning (separate tables, sharding), dispatcher scaling is expected to work

**Recommended Configuration (for this environment):**
- Dense workload + single table: Dispatcher=1, Worker=5-10, MaxConns=20
- With work partitioning: Horizontal dispatcher scaling viable

## 2. Methodology

### 2.1 Test Environment (Important Limitations)

**Infrastructure:**
- LocalStack SQS (not production AWS SQS)
- PostgreSQL 15 (Docker)
- Docker Compose (single host)
- GOMAXPROCS=1 (0.5 vCPU per container)

**Workload Characteristics:**
- Dense workload: All messages in single outbox table
- No work partitioning: All dispatcher instances compete for same message set
- Batch size: 10 messages
- Event type: user (single type for matrix tests)

**CRITICAL: These results are specific to this environment and workload.**

### 2.2 Measurement Methods
- Instrumented Repository for SELECT FOR UPDATE SKIP LOCKED metrics
- pg_stat_activity for database connection analysis
- Test patterns: Instance × Worker matrix, multiple consumer speeds

## 3. Results

### 3.1 Dispatcher Instance × Worker Matrix
| Instances | Workers | Throughput (msg/sec) | Avg Fetch (ms) | Empty Fetch % |
|-----------|---------|----------------------|----------------|---------------|
| 1         | 1       | 90                   | 1.2            | 15%           |
| 1         | 5       | 180                  | 0.8            | 25%           |
| ...       | ...     | ...                  | ...            | ...           |

### 3.2 Bottleneck Identification

**In this specific environment (LocalStack + dense workload + single outbox table):**

- **Batch Fragmentation**: Without work partitioning, increasing dispatcher instances causes severe batch fragmentation
  - Each instance competes for messages from the same table
  - SELECT FOR UPDATE SKIP LOCKED returns fewer messages per fetch
  - Throughput decreases as instances increase (d1: 728 msg/sec → d3: 67 msg/sec → d5: 37 msg/sec)
  - Empty fetch rate increases significantly with more instances

- **Connection Pool**: Worker=10 で wait duration 増加 → Connection limit が bottleneck

- **CPU**: GOMAXPROCS=1 制約により single-threaded → Multi-core 未活用

**This does NOT mean horizontal scaling is universally ineffective.** With proper work partitioning (e.g., separate outbox tables per service, sharding by tenant_id), dispatcher scaling can be effective.

### 3.3 Consumer Speed Impact
| Consumer Speed | Dispatcher=1 | Dispatcher=5 | Improvement |
|----------------|--------------|--------------|-------------|
| Fast (50ms)    | 200 msg/sec  | 220 msg/sec  | +10%        |
| Slow (2s)      | 10 msg/sec   | 45 msg/sec   | +350%       |

## 4. Conclusions

### 4.1 Key Findings (Specific to This Test Environment)

**Test Environment Recap:**
- LocalStack SQS (not production AWS)
- Single outbox table with all message types
- Dense workload (all messages compete for same table)
- No work partitioning implemented

**Findings:**

1. **Batch Fragmentation with Multiple Dispatcher Instances**
   - Without work partitioning, increasing dispatcher instances severely reduces throughput
   - Root cause: All instances compete for messages in single table via SELECT FOR UPDATE SKIP LOCKED
   - Result: Each instance gets fewer messages per batch, increasing overhead
   - Observed: d1=728 msg/sec → d3=67 msg/sec → d5=37 msg/sec (10-20x degradation)

2. **Secondary Bottlenecks**
   - Connection pool (MaxConns): Worker=10 で wait duration 増加
   - CPU limitation: GOMAXPROCS=1 prevents multi-core utilization

### 4.2 Recommended Configuration (For This Environment)

- **Dense workload + single outbox table**: Dispatcher=1, Worker=5-10, MaxConns=20
- **If work partitioning is possible**: Dispatcher horizontal scaling becomes viable

### 4.3 What This Does NOT Prove

❌ "Horizontal scaling of dispatchers is universally ineffective"
❌ "Multiple dispatcher instances always reduce throughput"
❌ "SELECT FOR UPDATE SKIP LOCKED cannot scale"

✅ **Actual finding**: "Without work partitioning, increasing dispatcher instances causes batch fragmentation in dense workloads, reducing throughput"

### 4.4 Work Partitioning Strategies (NOT tested but expected to enable scaling)

- Separate outbox tables per microservice
- Sharding by tenant_id or account_id
- Partitioning by event_type or date range
- Each dispatcher instance assigned to specific partition

## 5. やってはいけない構成 (Anti-patterns for This Environment)

**Specific to dense workload + single outbox table:**
- ❌ Multiple dispatcher instances without work partitioning
- ❌ Workers > MaxConns (connection starvation)
- ❌ Assuming these results apply to all architectures
```

## トラブルシューティング

### Instrumentation が動作しない

**確認事項**:
1. `dispatcher-bench` を使用しているか？
2. Log level が Info 以上か？
3. Messages が実際に処理されているか？

**Debug**:
```bash
# Verbose logging
LOG_LEVEL=DEBUG go run cmd/dispatcher-bench/main.go --workers 2
```

### Matrix benchmark が失敗する

**よくある原因**:
1. docker-compose.yml に `WORKERS` サポートが未実装
2. Container name の衝突
3. Port の競合

**修正**:
```bash
# 完全クリーンアップ
docker-compose down -v
docker system prune -f

# 再実行
./scripts/start.sh
./scripts/matrix-bench.sh
```

### PostgreSQL connection limit

**エラー**:
```
FATAL: sorry, too many clients already
```

**修正**:
```yaml
# docker-compose.yml
postgres:
  command: postgres -c max_connections=200
```

## 次のステップ（優先順位順）

1. **High**: PG Connection Metrics 実装 (`internal/instrumentation/pg_metrics.go`)
2. **High**: Mock Notification Handler 実装 (`cmd/consumer/handlers/mock.go`)
3. **Medium**: Long-running benchmark script (`scripts/long-bench.sh`)
4. **Medium**: docker-compose.yml に WORKERS サポート追加
5. **Low**: Automated report generation script
6. **Low**: Grafana dashboard for real-time metrics

## 参考資料

- [PostgreSQL pg_stat_activity documentation](https://www.postgresql.org/docs/current/monitoring-stats.html)
- [pgx connection pool](https://github.com/jackc/pgx/wiki/Connection-pool)
- [SELECT FOR UPDATE SKIP LOCKED behavior](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)

---

**作成日時**: 2025-12-15
**ステータス**: Instrumentation 実装完了 / Benchmark scripts 基盤完成 / 実行・分析フェーズ待ち
