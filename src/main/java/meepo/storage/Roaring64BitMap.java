package meepo.storage;

import org.roaringbitmap.RoaringBitmap;

import java.util.*;

/**
 * Created by peiliping on 17-7-21.
 */
public class Roaring64BitMap {

    private Map<Long, RoaringBitmap> core = new HashMap<>();

    public void add(Long data) {
        if (data == null)
            return;
        long seq = data >> 31;
        int val = (int) (data & Integer.MAX_VALUE);
        RoaringBitmap rb = this.core.get(seq);
        if (rb == null) {
            rb = new RoaringBitmap();
            this.core.put(seq, rb);
        }
        rb.add(val);
    }

    public long getCardinality() {
        long size = 0;
        for (Map.Entry<Long, RoaringBitmap> entry : this.core.entrySet())
            size += entry.getValue().getCardinality();
        return size;
    }

    public static Roaring64BitMap and(Roaring64BitMap r1, Roaring64BitMap r2) {
        Roaring64BitMap result = new Roaring64BitMap();
        Set<Long> resultKeys = new HashSet<>();
        resultKeys.addAll(r1.core.keySet());
        resultKeys.retainAll(r2.core.keySet());
        for (Long key : resultKeys) {
            result.core.put(key, RoaringBitmap.and(r1.core.get(key), r2.core.get(key)));
        }
        return result;
    }

    public static Roaring64BitMap xor(Roaring64BitMap r1, Roaring64BitMap r2) {
        Roaring64BitMap result = new Roaring64BitMap();
        Set<Long> resultKeys = new HashSet<>();
        resultKeys.addAll(r1.core.keySet());
        resultKeys.addAll(r2.core.keySet());
        for (Long key : resultKeys) {
            if (r1.core.containsKey(key)) {
                if (r2.core.containsKey(key)) {
                    result.core.put(key, RoaringBitmap.xor(r1.core.get(key), r2.core.get(key)));
                } else {
                    result.core.put(key, r1.core.get(key).clone());
                }
            } else {
                result.core.put(key, r2.core.get(key).clone());
            }
        }
        return result;
    }

    public static Roaring64BitMap or(Roaring64BitMap r1, Roaring64BitMap r2) {
        Roaring64BitMap result = new Roaring64BitMap();
        Set<Long> resultKeys = new HashSet<>();
        resultKeys.addAll(r1.core.keySet());
        resultKeys.addAll(r2.core.keySet());
        for (Long key : resultKeys) {
            if (r1.core.containsKey(key)) {
                if (r2.core.containsKey(key)) {
                    result.core.put(key, RoaringBitmap.or(r1.core.get(key), r2.core.get(key)));
                } else {
                    result.core.put(key, r1.core.get(key).clone());
                }
            } else {
                result.core.put(key, r2.core.get(key).clone());
            }
        }
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder answer = new StringBuilder();
        answer.append("{");
        Set<Long> keys = new TreeSet<>(this.core.keySet());
        for (Long key : keys) {
            Iterator<Integer> it = this.core.get(key).iterator();
            while (it.hasNext()) {
                answer.append((key << 31) + it.next());
                answer.append(",");
            }
        }
        answer.append("}");
        return answer.toString();
    }
}
