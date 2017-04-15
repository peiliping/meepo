package meepo.util.lrucache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;

/**
 * Created by peiliping on 17-4-15.
 */
public class LRUCache<V> {

    public static final long DEFAULT_EXPIRE = 1000 * 60 * 30;

    public static final long DEFAULT_NULL_EXPIRE = -1;

    public static final int DEFAULT_MAXSIZE = 5000;

    private long maxSize;

    private boolean ignoretime;

    private long expireTime;

    private long nullExpireTime;

    private ConcurrentLinkedHashMap<Object, CacheItem<V>> cache;

    public LRUCache() {
        this(DEFAULT_MAXSIZE, true);
    }

    public LRUCache(int maxsize, boolean ignoretime) {
        this(maxsize, ignoretime, DEFAULT_EXPIRE, DEFAULT_NULL_EXPIRE);
    }

    public LRUCache(int maxsize, long expiretime) {
        this(maxsize, false, expiretime, DEFAULT_NULL_EXPIRE);
    }

    public LRUCache(int maxsize, long expiretime, long nullexpiretime) {
        this(maxsize, false, expiretime, nullexpiretime);
    }

    public LRUCache(int maxsize, boolean ignoretime, long expiretime, long nullexpiretime) {
        this.maxSize = maxsize;
        this.ignoretime = ignoretime;
        this.expireTime = expiretime;
        this.nullExpireTime = nullexpiretime;
        this.cache = new ConcurrentLinkedHashMap.Builder<Object, CacheItem<V>>().maximumWeightedCapacity(maxsize).weigher(Weighers.singleton()).build();
    }

    public V get(Object key) {
        CacheItem<V> item = this.cache.get(key);
        if (item != null) {
            if (item.isTimeOut(this.expireTime, this.nullExpireTime)) {
                this.cache.remove(key);
            } else {
                return item.getValue();
            }
        }
        return null;
    }

    public V get(Object key, CacheFunc<V> function) {
        CacheItem<V> item = this.cache.get(key);
        if (item != null) {
            if (item.isTimeOut(this.expireTime, this.nullExpireTime)) {
                this.cache.remove(key);
            } else {
                return item.getValue();
            }
        }
        if (function != null) {
            V v = function.func();
            if (v != null) {
                putNotNull(key, v);
                return v;
            } else {
                if (this.nullExpireTime > 0) {
                    putNullCache(key);
                }
            }
        }
        return null;
    }

    private void putNullCache(Object key) {
        if (this.cache.size() > this.maxSize * 2) {
            return;
        }
        this.cache.put(key, new CacheItem<V>(null, this.ignoretime));
    }

    private void putNotNull(Object key, V value) {
        if (value == null) {
            return;
        }
        if (this.cache.size() > this.maxSize * 2) {
            return;
        }
        this.cache.put(key, new CacheItem<V>(value, this.ignoretime));
    }

    public void clear() {
        if (this.cache != null) {
            this.cache.clear();
        }
    }
}
