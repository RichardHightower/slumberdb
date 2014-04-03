package info.slumberdb;

import java.util.*;

public class InMemoryBinaryKeyValueStore implements KeyValueStore<byte[], byte[]> {


    SortedMap<byte[], byte[]> map = new TreeMap<>();

    @Override
    public void put(byte[] key, byte[] value) {
        map.put(key, value);
    }

    @Override
    public void putAll(Map<byte[], byte[]> values) {

        map.putAll(values);
    }

    @Override
    public void removeAll(Iterable<byte[]> keys) {
        for (byte[] key : keys) {
            map.remove(key);
        }
    }

    @Override
    public void updateAll(Iterable<CrudOperation> updates) {
        throw new RuntimeException("Not implemented");
    }



    @Override
    public void remove(byte[] key) {
        map.remove(key);
    }

    @Override
    public KeyValueIterable<byte[], byte[]> search(byte[] startKey) {

        final Set<Map.Entry<byte[], byte[]>> entries = map.tailMap(startKey).entrySet();
        final Iterator<Map.Entry<byte[], byte[]>> iterator = entries.iterator();
        return new KeyValueIterable<byte[], byte[]>() {
            @Override
            public void close() {

            }

            @Override
            public Iterator<Entry<byte[], byte[]>> iterator() {
                return new Iterator<Entry<byte[], byte[]>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<byte[], byte[]> next() {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        return new Entry<>(next.getKey(), next.getValue());
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }

    @Override
    public KeyValueIterable<byte[], byte[]> loadAll() {

        final Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
        final Iterator<Map.Entry<byte[], byte[]>> iterator = entries.iterator();
        return new KeyValueIterable<byte[], byte[]>() {
            @Override
            public void close() {

            }

            @Override
            public Iterator<Entry<byte[], byte[]>> iterator() {
                return new Iterator<Entry<byte[], byte[]>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<byte[], byte[]> next() {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        return new Entry<>(next.getKey(), next.getValue());
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }

    @Override
    public byte[] get(byte[] key) {
        return map.get(key);
    }

    @Override
    public void close() {

    }

    @Override
    public void flush() {

    }
}
