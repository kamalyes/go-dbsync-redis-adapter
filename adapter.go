/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-08 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-08 00:00:00
 * @FilePath: \go-dbsync-redis-adapter\adapter.go
 * @Description: go-dbsync 的 Redis adapter —— 水位（MSet 原子）/ 选主锁（SET NX PX + Lua 安全释放）/
 * bootstrap 进度游标，一个 Store 装配三件套；键布局 <prefix>:watermark:{updated_at,cursor_id} /
 * <prefix>:bootstrap:cursor / <prefix>:lock
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package redisadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	dbsync "github.com/kamalyes/go-dbsync"
	"github.com/redis/go-redis/v9"
)

// DefaultLockTTL 默认锁存活时间（必须 > 单次同步超时以覆盖整个同步窗口）
const DefaultLockTTL = 65 * time.Second

// Store Redis 存储：WatermarkStore + CursorStore + LockManager 三合一
//
// 键布局（prefix 建议含业务域与任务名，如 "dashboard:user_sync"）：
//   - <prefix>:watermark:updated_at / <prefix>:watermark:cursor_id（水位，MSet 原子）
//   - <prefix>:bootstrap:cursor（全量回补进度，24h 过期）
//   - <prefix>:lock（选主锁，SET NX PX）
type Store struct {
	rdb    redis.UniversalClient
	prefix string

	wmUpdatedAtKey string
	wmCursorKey    string
	cursorKey      string
	lockKey        string
	lockTTL        time.Duration
}

// NewStore 创建 Redis 存储（实现 dbsync.WatermarkStore / dbsync.CursorStore / dbsync.LockManager）
func NewStore(rdb redis.UniversalClient, prefix string) *Store {
	return &Store{
		rdb:            rdb,
		prefix:         prefix,
		wmUpdatedAtKey: prefix + ":watermark:updated_at",
		wmCursorKey:    prefix + ":watermark:cursor_id",
		cursorKey:      prefix + ":bootstrap:cursor",
		lockKey:        prefix + ":lock",
		lockTTL:        DefaultLockTTL,
	}
}

// WithLockTTL 设置锁存活时间（链式，覆盖默认 65s）
func (s *Store) WithLockTTL(ttl time.Duration) *Store {
	if ttl > 0 {
		s.lockTTL = ttl
	}
	return s
}

// ReadWatermark 读水位：两 key 分别读，miss（redis.Nil）返回 ok=false；网络错误必须报错
func (s *Store) ReadWatermark(ctx context.Context) (time.Time, string, bool, error) {
	updatedStr, err := s.rdb.Get(ctx, s.wmUpdatedAtKey).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, "", false, nil
	}
	if err != nil {
		return time.Time{}, "", false, err
	}
	cursorID, err := s.rdb.Get(ctx, s.wmCursorKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return time.Time{}, "", false, err
	}
	at, err := time.Parse(time.RFC3339Nano, updatedStr)
	if err != nil {
		// 水位损坏视为 miss（触发 bootstrap 重拉，幂等兜底）
		return time.Time{}, "", false, nil
	}
	return at, cursorID, true, nil
}

// WriteWatermark 写水位：MSet 单次原子写（updated_at 与 cursor_id 不可错配）
func (s *Store) WriteWatermark(ctx context.Context, watermarkAt time.Time, cursorID string) error {
	return s.rdb.MSet(ctx,
		s.wmUpdatedAtKey, watermarkAt.Format(time.RFC3339Nano),
		s.wmCursorKey, cursorID,
	).Err()
}

// ReadCursor 读 bootstrap 进度游标
func (s *Store) ReadCursor(ctx context.Context) (string, bool, error) {
	v, err := s.rdb.Get(ctx, s.cursorKey).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// WriteCursor 写 bootstrap 进度游标（24h 过期：全量回补中断过久则从头重拉，幂等兜底）
func (s *Store) WriteCursor(ctx context.Context, cursorID string) error {
	return s.rdb.Set(ctx, s.cursorKey, cursorID, 24*time.Hour).Err()
}

// DeleteCursor 清除 bootstrap 进度游标
func (s *Store) DeleteCursor(ctx context.Context) error {
	return s.rdb.Del(ctx, s.cursorKey).Err()
}

// Acquire 获取选主锁：SET NX PX，value 为实例唯一 token；释放走 Lua compare-and-del
func (s *Store) Acquire(ctx context.Context) (func(), bool, error) {
	token := lockToken()
	ok, err := s.rdb.SetNX(ctx, s.lockKey, token, s.lockTTL).Result()
	if err != nil {
		return nil, false, fmt.Errorf("redis SetNX(%s): %w", s.lockKey, err)
	}
	if !ok {
		return nil, false, nil
	}
	release := func() {
		// 仅删除自己持有的锁：防 TTL 先过期、他实例抢到新锁后被误删
		s.rdb.Eval(ctx,
			`if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end`,
			[]string{s.lockKey}, token)
	}
	return release, true, nil
}

// 接口断言：Store 实现 dbsync 三接口
var (
	_ dbsync.WatermarkStore = (*Store)(nil)
	_ dbsync.CursorStore    = (*Store)(nil)
	_ dbsync.LockManager    = (*Store)(nil)
)

// ---------- 锁 token 生成（实例内唯一即可） ----------

var tokenSeq atomic.Uint64

// lockToken 实例唯一 token：hostname+pid（进程重启必换）+ 单调序列（同进程并发不撞）
func lockToken() string {
	seq := tokenSeq.Add(1)
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d-%d", host, os.Getpid(), time.Now().UnixNano(), seq)
}
