package info.slumberdb;

import org.boon.Exceptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by Richard on 4/4/14.
 */
public class SimpleJavaSerializationKeyValueStoreMySQL <V extends Serializable>  implements SerializedJavaKeyValueStore<String,V> {

    private KeyValueStore <String, byte[]> store;

    public SimpleJavaSerializationKeyValueStoreMySQL(String url, String userName, String password, String table, Class<V> cls) {
        store = new SimpleStringBinaryKeyValueStoreMySQL(url, userName, password, table);
    }



    protected static String toString(byte [] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    private  V toObject(byte[] value) {
        V v = null;
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            v = (V)objectInputStream.readObject();
        } catch (Exception e) {
            Exceptions.handle(e);
        }
        return v;
    }


    byte[] toBytes(V v) {


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream streamOut = new ObjectOutputStream(baos);
            streamOut.writeObject(v);
        } catch (IOException e) {
            Exceptions.handle(e);
        }

        return baos.toByteArray();
    }




    @Override
    public void put(String key, V value) {
        store.put(key, toBytes(value));
    }

    @Override
    public void putAll(Map<String, V> values) {
        Set<Map.Entry<String, V>> entries = values.entrySet();
        Map<String, byte[]> map = new HashMap<>(values.size());

        for (Map.Entry<String, V> entry : entries) {
            map.put(entry.getKey(), toBytes(entry.getValue()));
        }

        store.putAll(map);
    }

    @Override
    public void removeAll(Iterable<String> keys) {
        store.removeAll( keys );
    }

    @Override
    public void remove(String key) {

        store.remove( key );
    }

    @Override
    public KeyValueIterable<String, V> search(String startKey) {
        final KeyValueIterable<String, byte[]> search = store.search(startKey);
        final Iterator<Entry<String, byte[]>> iterator = search.iterator();
        return new KeyValueIterable<String, V>() {
            @Override
            public void close() {
                search.close();
            }

            @Override
            public Iterator<Entry<String, V>> iterator() {
                return new Iterator<Entry<String, V>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, V> next() {
                        final Entry<String, byte[]> next = iterator.next();

                        return new Entry<>(next.key(),
                                toObject(next.value()));
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
    public KeyValueIterable<String, V> loadAll() {
        final KeyValueIterable<String, byte[]> search = store.loadAll();
        final Iterator<Entry<String, byte[]>> iterator = search.iterator();
        return new KeyValueIterable<String, V>() {
            @Override
            public void close() {
                search.close();
            }

            @Override
            public Iterator<Entry<String, V>> iterator() {
                return new Iterator<Entry<String, V>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, V> next() {
                        final Entry<String, byte[]> next = iterator.next();

                        return new Entry<>(next.key(),
                                toObject(next.value()));
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
    public Collection<String> loadAllKeys() {
        return store.loadAllKeys();
    }

    @Override
    public V load(String key) {
        final byte[] bytes = store.load(key);
        if (bytes != null) {
            return toObject(bytes);
        }else {
            return null;
        }
    }

    @Override
    public Map<String, V> loadAllByKeys(Collection<String> keys) {

        Set<String> keySet = new TreeSet<>(keys);

        final Map<String, byte[]> map = store.loadAllByKeys(keySet);
        final Map<String, V> results= new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            results.put(entry.getKey(), toObject(entry.getValue()));
        }

        return results;
    }


    @Override
    public void close() {
        store.close();

    }

}
