/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-08 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-08 00:00:00
 * @FilePath: \go-dbsync-redis-adapter\adapter_test.go
 * @Description: Store 单测（miniredis 内存 Redis）：水位读写 / bootstrap 游标生命周期 /
 * 选主锁互斥与安全释放（Lua compare-and-del）/ 水位损坏自愈
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package redisadapter

import (
	"context"
	"testing"
	"time"

	dbsync "github.com/kamalyes/go-dbsync"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStore(rdb, "test:sync"), mr
}

func TestWatermarkRoundtrip(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	// 初始 miss
	if _, _, ok, err := s.ReadWatermark(ctx); err != nil || ok {
		t.Fatalf("initial read: ok=%v err=%v", ok, err)
	}

	// 写读一致
	at := time.Now().UTC()
	if err := s.WriteWatermark(ctx, at, "u1"); err != nil {
		t.Fatal(err)
	}
	gotAt, gotID, ok, err := s.ReadWatermark(ctx)
	if err != nil || !ok || !gotAt.Equal(at) || gotID != "u1" {
		t.Fatalf("roundtrip: ok=%v err=%v at=%v id=%q", ok, err, gotAt, gotID)
	}

	// cursor_id 缺失（单 key 丢失）不视为整体 miss
	if err := s.rdb.Del(ctx, s.wmCursorKey).Err(); err != nil {
		t.Fatal(err)
	}
	if _, gotID, ok, err := s.ReadWatermark(ctx); err != nil || !ok || gotID != "" {
		t.Fatalf("cursor miss tolerated: ok=%v err=%v id=%q", ok, err, gotID)
	}
}

func TestWatermarkCorruptedTreatedAsMiss(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	if err := s.rdb.Set(ctx, s.wmUpdatedAtKey, "not-a-timestamp", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := s.ReadWatermark(ctx); err != nil || ok {
		t.Fatalf("corrupted watermark should be miss: ok=%v err=%v", ok, err)
	}
}

func TestCursorLifecycle(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	if _, ok, err := s.ReadCursor(ctx); err != nil || ok {
		t.Fatalf("initial read: ok=%v err=%v", ok, err)
	}
	if err := s.WriteCursor(ctx, "user-123"); err != nil {
		t.Fatal(err)
	}
	if c, ok, err := s.ReadCursor(ctx); err != nil || !ok || c != "user-123" {
		t.Fatalf("roundtrip: %q ok=%v err=%v", c, ok, err)
	}
	if err := s.DeleteCursor(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.ReadCursor(ctx); ok {
		t.Fatal("cursor should be deleted")
	}
}

func TestLockMutualExclusion(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	release, ok, err := s.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("first acquire: ok=%v err=%v", ok, err)
	}

	// 持有期间互斥
	if _, ok2, err := s.Acquire(ctx); err != nil || ok2 {
		t.Fatalf("second acquire must fail: ok=%v err=%v", ok2, err)
	}

	release()

	// 释放后可重新获取
	release2, ok, err := s.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("re-acquire after release: ok=%v err=%v", ok, err)
	}
	release2()
}

// TestLockSafeReleaseAfterExpiry 锁过期被他实例持有后，旧 release（Lua compare-and-del）不得误删
func TestLockSafeReleaseAfterExpiry(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	// 实例 A 拿锁并让 TTL 立即过期
	releaseA, ok, err := s.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("acquire A: ok=%v err=%v", ok, err)
	}
	mr.FastForward(2 * DefaultLockTTL)

	// 实例 B 拿到新锁
	releaseB, ok, err := s.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("acquire B after expiry: ok=%v err=%v", ok, err)
	}

	// A 的旧 release 不能删掉 B 的锁
	releaseA()

	// B 仍持有：第三次获取应失败
	if _, ok3, err := s.Acquire(ctx); err != nil || ok3 {
		t.Fatalf("B's lock must survive A's stale release: ok=%v err=%v", ok3, err)
	}
	releaseB()
}

// TestLockTokenUniqueness 同进程多次获取 token 不撞（防误删并发持锁场景）
func TestLockTokenUniqueness(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		tok := lockToken()
		if _, dup := seen[tok]; dup {
			t.Fatalf("duplicate token: %s", tok)
		}
		seen[tok] = struct{}{}
	}
}

// TestImplementsInterfaces Store 实现 dbsync 三接口
func TestImplementsInterfaces(t *testing.T) {
	s, _ := newTestStore(t)
	var _ dbsync.WatermarkStore = s
	var _ dbsync.CursorStore = s
	var _ dbsync.LockManager = s
}
