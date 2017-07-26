package meepo.util.hp.bit;

import org.roaringbitmap.RoaringBitmap;

import java.util.*;

/**
 * Created by peiliping on 17-7-21.
 */
public class Roaring64BitMap implements Iterable<Long>, Cloneable {

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

    public void remove(Long data) {
        if (data == null)
            return;
        long seq = data >> 31;
        int val = (int) (data & Integer.MAX_VALUE);
        RoaringBitmap rb = this.core.get(seq);
        if (rb != null) {
            rb.remove(val);
        }
    }

    public long getCardinality() {
        long size = 0;
        for (Map.Entry<Long, RoaringBitmap> entry : this.core.entrySet())
            size += entry.getValue().getCardinality();
        return size;
    }

    public void runOptimize() {
        for (Map.Entry<Long, RoaringBitmap> entry : this.core.entrySet())
            entry.getValue().runOptimize();
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

    public static Roaring64BitMap andNot(Roaring64BitMap r1, Roaring64BitMap r2) {
        Roaring64BitMap result = r1.clone();
        Set<Long> leftKeys = new HashSet<>();
        leftKeys.addAll(r1.core.keySet());
        leftKeys.removeAll(r2.core.keySet());
        for (Long key : leftKeys)
            result.core.put(key, r1.core.get(key).clone());
        Set<Long> crossKeys = new HashSet<>();
        crossKeys.addAll(r1.core.keySet());
        crossKeys.retainAll(r2.core.keySet());
        for (Long key : crossKeys) {
            RoaringBitmap t = RoaringBitmap.andNot(r1.core.get(key), r2.core.get(key));
            if (t.getCardinality() > 0)
                result.core.put(key, t);
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

    @Override
    public Roaring64BitMap clone() {
        Roaring64BitMap result = new Roaring64BitMap();
        for (Map.Entry<Long, RoaringBitmap> entry : this.core.entrySet())
            result.core.put(entry.getKey(), entry.getValue().clone());
        return result;
    }

    @Override
    public Iterator<Long> iterator() {
        return new Roaring64BitMapInterator();
    }

    final class Roaring64BitMapInterator implements Iterator<Long> {

        TreeSet<Long> keys;

        Iterator<Long> keysIterator;

        Long keyCurrent;

        Iterator<Integer> valsIterator;

        public Roaring64BitMapInterator() {
            this.keys = new TreeSet<>(core.keySet());
            this.keysIterator = this.keys.iterator();
            if (this.keysIterator.hasNext()) {
                this.keyCurrent = this.keysIterator.next();
                this.valsIterator = core.get(this.keyCurrent).iterator();
            }
        }

        @Override
        public boolean hasNext() {
            if (this.valsIterator == null) {
                return false;
            }
            if (this.valsIterator.hasNext()) {
                return true;
            }
            while (this.keysIterator.hasNext()) {
                this.keyCurrent = this.keysIterator.next();
                this.valsIterator = core.get(this.keyCurrent).iterator();
                if (this.valsIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Long next() {
            return (this.keyCurrent << 31) + this.valsIterator.next();
        }
    }
}
