package org.guoguo.consumer.dispatch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 消费端去重缓存（P1 Stage D）。
 */
class DedupCacheTest {

    @Test
    void detectsOnlyMarkedMessages() {
        DedupCache cache = new DedupCache(10);

        assertFalse(cache.isDuplicate("m1"));
        cache.mark("m1");

        assertTrue(cache.isDuplicate("m1"));
        assertFalse(cache.isDuplicate("m2"));
        assertEquals(1, cache.getHits());
    }

    @Test
    void evictsLeastRecentlyUsed() {
        DedupCache cache = new DedupCache(2);
        cache.mark("a");
        cache.mark("b");
        // 访问 a，使其成为最近使用；b 变为最旧
        assertTrue(cache.isDuplicate("a"));
        cache.mark("c");

        assertFalse(cache.isDuplicate("b"), "最久未使用的 b 应被淘汰");
        assertTrue(cache.isDuplicate("a"));
    }

    @Test
    void zeroCapacityIsDisabledByCaller() {
        DedupCache cache = new DedupCache(0);

        cache.mark("m1");
        assertFalse(cache.isDuplicate("m1"), "容量 0 应等价于关闭");
        assertEquals(0, cache.getCapacity());
    }
}
