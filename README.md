<div align="center">

# 🔴 Go-DBSYNC Redis Adapter

**go-dbsync 的 Redis 存储适配器 - 水位 / 选主锁 / bootstrap 游标三合一**

*水位 MSet 原子写、SET NX PX 选主锁 + Lua 安全释放、bootstrap 进度游标，一个 Store 装配三件套*

<br>

[![stable](https://img.shields.io/badge/stable-stable-green.svg)](https://github.com/kamalyes/go-dbsync-redis-adapter)
[![license](https://img.shields.io/github/license/kamalyes/go-dbsync-redis-adapter)]()
[![download](https://img.shields.io/github/downloads/kamalyes/go-dbsync-redis-adapter/total)]()
[![release](https://img.shields.io/github/v/release/kamalyes/go-dbsync-redis-adapter)]()
[![commit](https://img.shields.io/github/last-commit/kamalyes/go-dbsync-redis-adapter)]()
[![issues](https://img.shields.io/github/issues/kamalyes/go-dbsync-redis-adapter)]()
[![pull](https://img.shields.io/github/issues-pr/kamalyes/go-dbsync-redis-adapter)]()
[![fork](https://img.shields.io/github/forks/kamalyes/go-dbsync-redis-adapter)]()
[![star](https://img.shields.io/github/stars/kamalyes/go-dbsync-redis-adapter)]()
[![go](https://img.shields.io/github/go-mod/go-version/kamalyes/go-dbsync-redis-adapter)]()
[![size](https://img.shields.io/github/repo-size/kamalyes/go-dbsync-redis-adapter)]()
[![contributors](https://img.shields.io/github/contributors/kamalyes/go-dbsync-redis-adapter)]()
[![Go Report Card](https://goreportcard.com/badge/github.com/kamalyes/go-dbsync-redis-adapter)](https://goreportcard.com/report/github.com/kamalyes/go-dbsync-redis-adapter)
[![Go Reference](https://pkg.go.dev/badge/github.com/kamalyes/go-dbsync-redis-adapter?status.svg)](https://pkg.go.dev/github.com/kamalyes/go-dbsync-redis-adapter?tab=doc)

<br>

*[API 文档](https://pkg.go.dev/github.com/kamalyes/go-dbsync-redis-adapter)*
</div>

## 🚀 为什么选择 go-dbsync-redis-adapter？

> 为 [go-dbsync](https://github.com/kamalyes/go-dbsync) 提供 Redis 侧的全部存储能力：**水位**（主读路径）、**选主锁**（多实例单活）、**bootstrap 进度游标**（全量回补崩溃续拉），一个 `Store` 实例实现三个接口。

### ⚡ 核心特性

- **💧 水位原子写**: `MSet` 单次原子写 `updated_at` 与 `cursor_id`（两 key 不可错配）
- **🔒 选主锁安全**: `SET NX PX` + 实例唯一 token；释放走 Lua compare-and-del，防 TTL 先过期误删他实例新锁
- **📍 bootstrap 游标**: 进度 24h TTL——全量回补中断过久自动从头重拉（幂等兜底）
- **🧪 内存测试**: miniredis 全链路测试，`-race` 全绿

### 键布局

| 键 | 用途 |
| --- | --- |
| `<prefix>:watermark:updated_at` | 水位时间（RFC3339Nano） |
| `<prefix>:watermark:cursor_id` | 水位消歧 ID |
| `<prefix>:bootstrap:cursor` | 全量回补进度游标（24h TTL） |
| `<prefix>:lock` | 选主锁（TTL 默认 65s） |

## 📦 快速开始

### 环境要求

需要 [Go](https://go.dev/) 版本 [1.25.0](https://go.dev/doc/devel/release) 或更高版本，以及 [go-dbsync](https://github.com/kamalyes/go-dbsync)

### 安装

```sh
go get -u github.com/kamalyes/go-dbsync-redis-adapter
```

## 🚀 使用示例

```go
package main

import (
	"context"
	"time"

	dbsync "github.com/kamalyes/go-dbsync"
	redisadapter "github.com/kamalyes/go-dbsync-redis-adapter"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// 一个 Store 实现三个接口：WatermarkStore / CursorStore / LockManager
	store := redisadapter.NewStore(rdb, "myapp:user_sync")
	store = store.WithLockTTL(90 * time.Second) // 可选：覆盖默认锁 TTL 65s（必须 > 单次同步超时）

	syncer, err := dbsync.NewSyncer[UserRow](
		"user_sync",
		&userSource{},
		&userSink{},
		dbsync.NewCompositeWatermarkStore(store, gormCheckpoint), // Redis 主读 + DB 兜底
		store,                                                       // 选主锁
		dbsync.WithFullBootstrap[UserRow](store, 5*time.Minute),    // bootstrap 游标也用 Redis
		dbsync.WithSelfHeal[UserRow](targetTable),
	)
	if err != nil {
		panic(err)
	}
	_ = syncer
	_ = context.Background()
}
```

## 📋 API 速览

| 方法 | 说明 |
| --- | --- |
| `NewStore(rdb, prefix)` | 创建三合一 Store（锁 TTL 默认 65s） |
| `(*Store).WithLockTTL(ttl)` | 链式覆盖锁 TTL（必须 > 单次同步超时） |
| `ReadWatermark(ctx)` | 读水位（miss 返回 ok=false；损坏视为 miss 触发重拉） |
| `WriteWatermark(ctx, at, cursorID)` | 水位 MSet 原子写 |
| `ReadCursor / WriteCursor / DeleteCursor` | bootstrap 进度游标生命周期 |
| `Acquire(ctx)` | 选主锁（返回 release + 是否成功） |

## 🧪 测试

```sh
go test ./... -race -count=1
```

## 🤝 社区贡献

- **Fork 项目** → 创建特性分支 → 编写代码和测试 → 提交 PR
- 运行 `go test ./... -race` 确保所有测试通过

## ⭐ Star 历史

[![Star History Chart](https://api.star-history.com/svg?repos=kamalyes/go-dbsync-redis-adapter&type=Date)](https://star-history.com/#kamalyes/go-dbsync-redis-adapter&Date)

## 许可证

该项目使用 MIT 许可证，详见 [LICENSE](LICENSE) 文件
