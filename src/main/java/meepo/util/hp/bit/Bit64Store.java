package meepo.util.hp.bit;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peiliping on 17-7-21.
 */
public class Bit64Store {

    private static volatile Bit64Store INSTANCE;

    private ConcurrentMap<String, Roaring64BitMap> store;

    private Bit64Store() {
        this.store = new ConcurrentHashMap<>();
    }

    public static synchronized Bit64Store getInstance() {
        if (INSTANCE == null)
            INSTANCE = new Bit64Store();
        return INSTANCE;
    }

    public void save(String name, Roaring64BitMap rb) {
        INSTANCE.store.put(name, rb);
    }

    public long getSize(String name) {
        return INSTANCE.store.get(name) != null ? INSTANCE.store.get(name).getCardinality() : 0;
    }

    public Set<String> keys() {
        return INSTANCE.store.keySet();
    }

    public String diff(String s1, String s2) {
        Roaring64BitMap rb1 = INSTANCE.store.get(s1);
        Roaring64BitMap rb2 = INSTANCE.store.get(s2);
        if (rb1 != null && rb2 != null) {
            Roaring64BitMap rb = Roaring64BitMap.xor(rb1, rb2);
            return rb.toString();
        } else {
            return "{}";
        }
    }

    public void clean(String name) {
        INSTANCE.store.remove(name);
    }

}
