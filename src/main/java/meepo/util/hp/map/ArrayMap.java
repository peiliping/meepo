package meepo.util.hp.map;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * Created by peiliping on 17-6-30.
 */
public class ArrayMap<K, V> implements Map<K, V> {

    private Keys<K> keys;

    private V[] vals;

    private short len = 0;
    private transient Set<Map.Entry<K, V>> entrySet;

    public ArrayMap(Keys<K> keys) {
        if (keys == null || keys.size() == 0) {
            throw new IllegalArgumentException("ArrayMap Init Error : keys is empty .");
        }
        if (keys.size() > 128) {
            throw new IllegalArgumentException("ArrayMap Init Error : keys is too big .");
        }
        this.keys = keys;
        this.vals = (V[]) new Object[keys.size()];
    }

    @Override
    public int size() {
        return this.len;
    }

    @Override
    public boolean isEmpty() {
        return this.len == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        Integer p = this.keys.getPosByKey((K) key);
        return p != null && this.vals[p] != null;
    }

    @Override
    public boolean containsValue(Object value) {
        if (this.len == 0 || value == null)
            return false;
        for (int i = 0; i < this.vals.length; i++) {
            if (this.vals[i] != null && this.vals[i].equals(value))
                return true;
        }
        return false;
    }

    @Override
    public V get(Object key) {
        Integer p = this.keys.getPosByKey((K) key);
        return p != null ? this.vals[p] : null;
    }

    public V getP(Pair<Integer, K> p) {
        return this.vals.length > p.getLeft() ? this.vals[p.getLeft()] : null;
    }

    @Override
    public V put(K key, V value) {
        Integer p = this.keys.getPosByKey((K) key);
        if (p == null || value == null)
            return null;
        V v = this.vals[p];
        this.vals[p] = value;
        if (v == null)
            this.len++;
        return v;
    }

    public V putP(Pair<Integer, K> p, V value) {
        if (value == null || this.vals.length <= p.getLeft())
            return null;
        V v = this.vals[p.getLeft()];
        this.vals[p.getLeft()] = value;
        if (v == null)
            this.len++;
        return v;
    }

    @Override
    public V remove(Object key) {
        Integer p = this.keys.getPosByKey((K) key);
        if (p == null)
            return null;
        V v = this.vals[p];
        if (v != null)
            this.len--;
        this.vals[p] = null;
        return v;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> ent : m.entrySet())
            put(ent.getKey(), ent.getValue());
    }

    @Override
    public void clear() {
        this.vals = (V[]) new Object[keys.size()];
        this.len = 0;
    }

    @Override
    public Set<K> keySet() {
        return this.keys.keysSet();
    }

    @Override
    public Collection<V> values() {
        ArrayList<V> list = new ArrayList<V>(this.len);
        if (this.len > 0) {
            for (int i = 0; i < this.vals.length; i++)
                if (vals != null)
                    list.add((V) this.vals[i]);
        }
        return list;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        if (this.entrySet == null)
            this.entrySet = new EntrySet();
        return this.entrySet;
    }

    final class EntrySet extends AbstractSet<Map.Entry<K, V>> {

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public int size() {
            return ArrayMap.this.size();
        }
    }

    final class EntryIterator implements Iterator<Map.Entry<K, V>> {

        private EntryX entryx;

        public EntryIterator() {
            this.entryx = new EntryX(-1);
        }

        @Override
        public boolean hasNext() {
            return this.entryx.getPos() < vals.length - 1;
        }

        @Override
        public EntryX next() {
            return this.entryx.next();
        }
    }

    final class EntryX implements Map.Entry<K, V> {

        private int pos;

        public EntryX(int p) {
            this.pos = p;
        }

        @Override
        public K getKey() {
            return keys.getKeyByPos(this.pos);
        }

        @Override
        public V getValue() {
            return vals[this.pos];
        }

        public EntryX next() {
            this.pos++;
            return this;
        }

        public int getPos() {
            return this.pos;
        }

        @Override
        public Object setValue(Object value) {
            return putP(Pair.of(this.pos, keys.getKeyByPos(this.pos)), (V) value);
        }
    }
}
