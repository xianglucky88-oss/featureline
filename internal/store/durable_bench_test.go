package store

import (
	"context"
	"fmt"
	"testing"
)

// 写入成本应与数据集规模无关：追加一条 WAL 记录即可。
func BenchmarkSet(b *testing.B) {
	for _, n := range []int{0, 10_000} {
		b.Run(fmt.Sprintf("existing_keys=%d", n), func(b *testing.B) {
			s, err := Open(b.TempDir(), WithCompactionThreshold(64<<20))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			ctx := context.Background()
			for i := 0; i < n; i++ {
				if err := s.Set(ctx, "U", fmt.Sprintf("f%d", i), testValue(float64(i))); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Set(ctx, "U", "hot", testValue(float64(i))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// 在线读取只取读锁，与 WAL 体积、压缩无关。
func BenchmarkMGet(b *testing.B) {
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	names := []string{}
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("f%d", i)
		names = append(names, name)
		if err := s.Set(ctx, "U", name, testValue(float64(i))); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := s.MGet(ctx, "U", names); err != nil {
				b.Fatal(err)
			}
		}
	})
}
