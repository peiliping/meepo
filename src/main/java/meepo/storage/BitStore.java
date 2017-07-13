package meepo.storage;

import org.roaringbitmap.RoaringBitmap;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peiliping on 17-7-13.
 */
public class BitStore {

    private static BitStore INSTANCE;

    private ConcurrentMap<String, RoaringBitmap> store;

    private BitStore() {
        this.store = new ConcurrentHashMap<>();
    }

    public static synchronized BitStore getInstance() {
        if (INSTANCE == null)
            INSTANCE = new BitStore();
        return INSTANCE;
    }

    public void save(String name, RoaringBitmap rb) {
        INSTANCE.store.put(name, rb);
    }

    public int getSize(String name) {
        return INSTANCE.store.get(name).getCardinality();
    }

    public Set<String> list() {
        return INSTANCE.store.keySet();
    }

    public String diff(String s1, String s2) {
        RoaringBitmap rb1 = INSTANCE.store.get(s1);
        RoaringBitmap rb2 = INSTANCE.store.get(s2);
        RoaringBitmap rb = RoaringBitmap.xor(rb1, rb2);
        return rb.toString();
    }

    public void clean(String name){
        RoaringBitmap rb = INSTANCE.store.remove(name);
        rb.clear();
    }

}
