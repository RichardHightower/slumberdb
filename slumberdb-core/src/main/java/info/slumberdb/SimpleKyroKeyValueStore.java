package info.slumberdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.boon.Exceptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * This marries a key value store with Kyro for serializer and deserializer support.
 * It is a decorator. The real storage is done by the KeyValueStore <byte[], byte[]> store.
 * You specify the object type.
 *
 * This class is not thread safe, but the KeyValueStore <byte[], byte[]> likely is.
 * You need a SimpleKyroKeyValueStore per thread.
 * This is needed to optimize buffer reuse of Kyro.
 *
 * You can combine this Kyro store with any KeyValueStore <byte[], byte[]> store.
 *
 * It expects the key to be a simple string and the value to be an object that will be serialized using Kyro.
 *
 * @see info.slumberdb.KeyValueStore
 * @param <V> type of value we are storing.
 */
public class SimpleKyroKeyValueStore <V extends Serializable> implements SerializedJavaKeyValueStore<String,V> {

    /** Store that does the actual writing to DB (likely). */
    private final KeyValueStore <byte[], byte[]> store;

    /** Kyro serializer/deserializer */
    private final Kryo kryo = new Kryo();

    /** Type of class you are reading/writing. */
    private final Class<V> type;

    /**
     *
     * @param store store
     * @param type type
     */
    public SimpleKyroKeyValueStore(final KeyValueStore <byte[], byte[]> store,
                                   final Class<V> type) {
        this.store = store;
        this.type = type;
    }


    /**
     * Convert a binary array to a String.
     * @param key key
     * @return
     */
    protected static String toString(byte [] key) {
        return new String(key, StandardCharsets.UTF_8);
    }

    /**
     * Use Kyro to read this byte array as an object.
     * @param value value read
     * @return new object read from key/value store
     */
    private  V  toObject(byte[] value) {
        if (value == null || value.length == 0) {return null;}
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


    /**
     * Converts an object to a byte array.
     * @param v object to convert
     * @return byte array representation of object.
     */
    byte[] toBytes(V v) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output streamOut = new Output(baos);
        this.kryo.writeObject(streamOut, v);
        streamOut.close();
        return baos.toByteArray();
    }


    /**
     * Convert a String key to bytes.
     * @param key key to convert
     * @return value
     */
    byte[] toBytes(String key) {

        return key.getBytes(StandardCharsets.UTF_8);
    }


    /**
     * Put a value in the key/value store.
     * @param key  key
     * @param value value
     */
    @Override
    public void put(String key, V value) {
        store.put(toBytes(key), toBytes(value));
    }

    /**
     * Put all of these values in the key value store.
     * @param values values
     */
    @Override
    public void putAll(Map<String, V> values) {
        Set<Map.Entry<String, V>> entries = values.entrySet();
        Map<byte[], byte[]> map = new HashMap<>(values.size());

        for (Map.Entry<String, V> entry : entries) {
            map.put(toBytes(entry.getKey()), toBytes(entry.getValue()));
        }

        store.putAll(map);
    }

    /**
     * Remove all of these values from the key value store.
     * @param keys
     */
    @Override
    public void removeAll(Iterable<String> keys) {
        List<byte[]> list = new ArrayList<>();

        for (String key : keys) {
            list.add(toBytes(key));
        }

        store.removeAll(list);
    }

    /**
     * Remove a key from the store.
     * @param key
     */
    @Override
    public void remove(String key) {

        store.remove(toBytes(key));
    }

    /**
     * Search for a key in the key / value store.
     * @param startKey
     * @return
     */
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


    /**
     * Load all of the key / values from the store.
     * @return
     */
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

    /**
     * Get a value from the key value store.
     * @param key key
     * @return
     */
    @Override
    public V get(String key) {
        final byte[] bytes = store.get(toBytes(key));
        if (bytes != null) {
            return toObject(bytes);
        }else {
            return null;
        }
    }

    /** Close the store. */
    @Override
    public void close() {
        store.close();

    }
}
