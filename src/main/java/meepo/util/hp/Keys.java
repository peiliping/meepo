package meepo.util.hp;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by peiliping on 17-6-30.
 */
public class Keys<K> {

    private transient Map<K, Integer> kp = new HashMap<>();

    private K[]                       pk;

    public Keys(K... pk) {
        if (pk == null || pk.length == 0) {
            throw new IllegalArgumentException("Keys Init Error : Empty");
        }
        this.pk = pk;
        for (int i = 0; i < pk.length; i++) {
            this.kp.put(pk[i], i);
        }
    }

    public Keys(Pair<Integer, K>... items) {
        if (items == null || items.length == 0) {
            throw new IllegalArgumentException("Keys Init Error : Empty");
        }
        this.pk = (K[]) new Object[items.length];
        for (Pair<Integer, K> item : items)
            this.pk[item.getLeft()] = item.getRight();
    }

    public Integer getPosByKey(K k) {
        return this.kp.get(k);
    }

    public K getKeyByPos(Integer i) {
        return this.pk[i];
    }

    public int size() {
        return pk.length;
    }

    public Set<K> keysSet() {
        return this.kp.keySet();
    }

}
