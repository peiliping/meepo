package meepo.storage;

import org.roaringbitmap.RoaringBitmap;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peiliping on 17-7-13.
 */
public class Bit32Store {

    private static volatile Bit32Store INSTANCE;

    private ConcurrentMap<String, RoaringBitmap> store;

    private Bit32Store() {
        this.store = new ConcurrentHashMap<>();
    }

    public static synchronized Bit32Store getInstance() {
        if (INSTANCE == null)
            INSTANCE = new Bit32Store();
        return INSTANCE;
    }

    public void save(String name, RoaringBitmap rb) {
        INSTANCE.store.put(name, rb);
    }

    public int getSize(String name) {
        return INSTANCE.store.get(name) != null ? INSTANCE.store.get(name).getCardinality() : 0;
    }

    public Set<String> keys() {
        return INSTANCE.store.keySet();
    }

    public String diff(String s1, String s2) {
        RoaringBitmap rb1 = INSTANCE.store.get(s1);
        RoaringBitmap rb2 = INSTANCE.store.get(s2);
        if (rb1 != null && rb2 != null) {
            RoaringBitmap rb = RoaringBitmap.xor(rb1, rb2);
            return rb.toString();
        } else {
            return "{}";
        }
    }

    public void clean(String name) {
        INSTANCE.store.remove(name);
    }
}
