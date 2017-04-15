package meepo.util.lrucache;

import lombok.Getter;

/**
 * Created by peiliping on 17-4-15.
 */
public class CacheItem<V> {

    @Getter private V value;

    private long createTime;

    private boolean ignoreTime;

    public CacheItem(V value, boolean ignoreTime) {
        this.value = value;
        this.ignoreTime = ignoreTime;
        this.createTime = this.ignoreTime ? 0 : System.currentTimeMillis();
    }

    public boolean isTimeOut(long expireTime, long nullExpireTime) {
        if (this.ignoreTime)
            return false;
        if (this.value == null && nullExpireTime > 0)
            return isTimeOut(nullExpireTime);
        return isTimeOut(expireTime);
    }

    public boolean isTimeOut(long expireTime) {
        return expireTime == Long.MAX_VALUE ? false : (System.currentTimeMillis() - createTime > expireTime);
    }

}
