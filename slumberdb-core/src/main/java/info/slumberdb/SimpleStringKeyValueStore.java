package info.slumberdb;

import org.boon.cache.SimpleCache;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.boon.Str.str;
import static org.boon.primitive.Byt.bytes;

/**
 * This represents a simple key / value store for strings.
 * You can combine this with any key value binary store (KeyValueStore<byte[], byte[]>).
 * It will encode the strings using UTF-8 encoding.
 */
public class SimpleStringKeyValueStore implements StringKeyValueStore {

    protected KeyValueStore<byte[], byte[]> store;
    protected SimpleCache<String, byte[]> keyCache = new SimpleCache<>(1_000);




    @Override
    public void put(String key, String value) {
        store.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void putAll(Map<String, String> values) {
        Map<byte[], byte[]>  map = new HashMap<>();

        for (Map.Entry<String, String> entry : values.entrySet()) {
            byte[] key = bytes(entry.getKey());
            byte[] value = bytes(entry.getValue());
            map.put(key, value);
        }

        store.putAll(map);
    }

    @Override
    public void removeAll(Iterable<String> keys) {
        List<byte[]> keyBytes = new ArrayList<>();

        for (String key : keys) {
            keyBytes.add(bytes(key));
        }

        store.removeAll(keyBytes);
    }


    @Override
    public void remove(String key) {
        store.remove(bytes(key));
    }



    @Override
    public KeyValueIterable<String, String> search(final String startKey
                                                   ) {

        final KeyValueIterable<byte[], byte[]> iterable = store.search(bytes(startKey));



        return new KeyValueIterable<String, String>(){
            @Override
            public void close() {
                iterable.close();
            }

            @Override
            public Iterator<Entry<String, String>> iterator() {
                final Iterator<Entry<byte[], byte[]>> iterator = iterable.iterator();


                return new Iterator<Entry<String, String>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, String> next() {
                        Entry<byte[], byte[]> current;

                        current = iterator.next();
                        String key = str(current.key());
                        String value = str(current.value());
                        Entry<String, String> entry = new Entry<>(key, value);

                        return entry;
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        } ;
    }

    @Override
    public KeyValueIterable<String, String> loadAll() {

        final KeyValueIterable<byte[], byte[]> iterable = store.loadAll();



        return new KeyValueIterable<String, String>(){
            @Override
            public void close() {
                iterable.close();
            }

            @Override
            public Iterator<Entry<String, String>> iterator() {
                final Iterator<Entry<byte[], byte[]>> iterator = iterable.iterator();


                return new Iterator<Entry<String, String>>() {
                    Entry<String, String> current;
                    @Override
                    public boolean hasNext() {

                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, String> next() {
                        Entry<byte[], byte[]> current;

                        current = iterator.next();
                        String key = str(current.key());
                        String value = str(current.value());
                        Entry<String, String> entry = new Entry<>(key, value);

                        this.current = entry;
                        return entry;
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        } ;
    }

    @Override
    public Collection<String> loadAllKeys() {
        final Collection<byte[]> keys = store.loadAllKeys();

        final Set<String> set = new HashSet<>();

        for (byte[] key : keys) {
            set.add(SimpleJavaSerializationStore.toString(key));
        }

        return set;
    }

    @Override
    public String get(String key) {
        byte[] bytes = store.get( keyToBytes(key) );
        if (bytes==null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private byte[] keyToBytes(String key) {
        byte[] value = keyCache.get(key);
        if (value == null) {
            value = key.getBytes(StandardCharsets.UTF_8);
            keyCache.put(key, value);
        }
        return value;
    }


    @Override
    public void close()  {
        store.close();
    }

}

