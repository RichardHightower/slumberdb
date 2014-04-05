package info.slumberdb;

import com.esotericsoftware.kryo.Kryo;
import org.boon.Exceptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by Richard on 4/5/14.
 */
public class SimpleKyroKeyValueStore <V extends Serializable> implements SerializedJavaKeyValueStore<String,V> {

    private KeyValueStore <byte[], byte[]> store;
    private Kryo kryo = new Kryo(); //TODO left off here.

    public SimpleKyroKeyValueStore(KeyValueStore <byte[], byte[]> store) {
        this.store = store;

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


    byte[] toBytes(String key) {

        return key.getBytes(StandardCharsets.UTF_8);
    }


    @Override
    public void put(String key, V value) {
        store.put(toBytes(key), toBytes(value));
    }

    @Override
    public void putAll(Map<String, V> values) {
        Set<Map.Entry<String, V>> entries = values.entrySet();
        Map<byte[], byte[]> map = new HashMap<>(values.size());

        for (Map.Entry<String, V> entry : entries) {
            map.put(toBytes(entry.getKey()), toBytes(entry.getValue()));
        }

        store.putAll(map);
    }

    @Override
    public void removeAll(Iterable<String> keys) {
        List<byte[]> list = new ArrayList<>();

        for (String key : keys) {
            list.add(toBytes(key));
        }

        store.removeAll(list);
    }

    @Override
    public void remove(String key) {

        store.remove(toBytes(key));
    }

    @Override
    public KeyValueIterable<String, V> search(String startKey) {
        final KeyValueIterable<byte[], byte[]> search = store.search(toBytes(startKey));
        final Iterator<Entry<byte[], byte[]>> iterator = search.iterator();
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
                        final Entry<byte[], byte[]> next = iterator.next();

                        return new Entry<>(SimpleJavaSerializationStore.toString(next.key()),
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
        final KeyValueIterable<byte[], byte[]> search = store.loadAll();
        final Iterator<Entry<byte[], byte[]>> iterator = search.iterator();
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
                        final Entry<byte[], byte[]> next = iterator.next();

                        return new Entry<>(SimpleJavaSerializationStore.toString(next.key()),
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
        final byte[] bytes = store.get(toBytes(key));
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
