package info.slumberdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.boon.Exceptions;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by Richard on 4/5/14.
 */
public class SimpleKyroKeyValueStoreMySQL <V extends Serializable>  implements SerializedJavaKeyValueStore<String,V> {

    private KeyValueStore <String, byte[]> store;
    private Class<V> type;
    private final Kryo kryo = new Kryo();


    public SimpleKyroKeyValueStoreMySQL(String url, String userName, String password, String table, Class<V> cls) {
        store = new SimpleStringBinaryKeyValueStoreMySQL(url, userName, password, table);
        this.type = cls;
    }



    private  V toObject(byte[] value) {
        if (value == null || value.length == 0) {
            return null;
        }
        V v = null;
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
        try {
            Input input = new Input(inputStream);
            v = kryo.readObject(input, type);
            input.close();
        } catch (Exception e) {
            Exceptions.handle(e);
        }
        return v;
    }


    byte[] toBytes(V v) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        this.kryo.writeObject(output, v);
        output.close();
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
    public V get(String key) {
        final byte[] bytes = store.get( key );
        if (bytes != null) {
            return toObject(bytes);
        }else {
            return null;
        }
    }

    @Override
    public void close() {
        store.close();

    }

}
