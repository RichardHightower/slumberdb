package info.slumberdb;

import org.boon.Str;
import org.boon.json.JsonParserAndMapper;
import org.boon.json.JsonParserFactory;
import org.boon.json.serializers.impl.JsonSimpleSerializerImpl;

import java.util.*;


/**
 * This marries a store to the Boon JSON parser and the Boon JSON serializer.
 * It is a decorator. The real storage is done by the StringKeyValueStore store.
 * You specify the object type.
 *
 * This class is not thread safe, but the StringKeyValueStore likely is.
 * You need a SimpleJsonKeyValueStore per thread.
 * This is needed to optimize buffer reuse of parser and serializer.
 *
 * You can combine This JSON store with any StringKeyValueStore store.
 *
 * It expects the key to be a simple string and the value to be an object that will be serialized to JSON.
 *
 * @see info.slumberdb.StringKeyValueStore
 * @param <V> type of value we are storing.
 */
public class SimpleJsonKeyValueStore<V> implements JsonKeyValueStore<String, V> {

    /** The type of object that we are serializing. */
    protected final Class<V> type;

    /** JSON serializer we are using. */
    protected JsonSimpleSerializerImpl serializer = new JsonSimpleSerializerImpl();

    /** JSON parser/deserializer that we are using. */
    protected JsonParserAndMapper deserializer = new JsonParserFactory().create();

    /** Key Value Store that does the actual storage. */
    protected StringKeyValueStore store;

    /** Key prefix which is useful if you are using a db like LevelDB and you
     * are storing more than one object in the same store so that objects can
     * be grouped for scanning, and searching.
     */
    protected final String keyPrefix;


    /**
     * Constructor to create a key / value store.
     * @param store store that does the actual store.
     * @param cls the class of the object that you are storing.
     */
    public SimpleJsonKeyValueStore(StringKeyValueStore store, Class<V> cls) {
        this(null, store, cls);
    }

    /**
     * Allows passing of a key prefix.
     * @param keyPrefix keyPrefix (useful if you are using LevelDB and are storing multiple objects in the same db.
     * @param store store that does the actual store.
     * @param cls the class of the object that you are storing.
     */
    public SimpleJsonKeyValueStore(String keyPrefix, StringKeyValueStore store, Class<V> cls) {
        type = cls;
        this.store = store;
        this.keyPrefix = keyPrefix;

    }

    /**
     * Converts an object to JSON.
     * @param value
     * @return
     */
    private String toJson(V value) {
        return serializer.serialize(value).toString();
    }

    /**
     * Used to store an object into long term storage.
     * The value will be converted to JSON before storage.
     * @param key  key
     * @param value value
     */
    @Override
    public void put(String key, V value) {

        store.put(prepareKey(key), toJson(value));

    }

    /**
     * Prepare a key for storage.
     * This adds the key prefix if set.
     * @param key key to prepare
     * @return prepared key
     */
    private String prepareKey(String key) {
        if (keyPrefix!=null) {
            return Str.add(keyPrefix, ".", key);
        } else {
            return key;
        }
    }

    /**
     * Puts all of the items into the store.
     * @param values values
     */
    @Override
    public void putAll(Map<String, V> values) {

        Map<String, String>  map = new HashMap<>();

        for (Map.Entry<String, V> entry : values.entrySet()) {
            String key = entry.getKey();
            V v = entry.getValue();
            String value = toJson(v);
            map.put(prepareKey(key), value);
        }

        store.putAll(map);

    }

    /**
     * Removes a group of keys from the underlying store.
     * @param keys keys to remove
     */
    @Override
    public void removeAll(Iterable<String> keys) {

        if (keyPrefix!=null) {

            List keysPrepared = new ArrayList();

            for (String key : keys) {
                keysPrepared.add(prepareKey(key));
            }
            store.removeAll(keysPrepared);

        } else {
            store.removeAll(keys);

        }

    }


    /**
     * Key to remove.
     * @param key key to remove
     */
    @Override
    public void remove(String key) {
        store.remove(key);
    }


    /**
     * Search by key returns iterable of objects.
     * @param start key or key fragment to search
     * @return an iteration over the keys and values.
     */
    @Override
    public KeyValueIterable<String, V> search(final String start) {

        final String startKey = prepareKey(start);



        final KeyValueIterable<String, String> iterable;

        iterable = store.search(startKey);



        return new KeyValueIterable<String, V>(){
            @Override
            public void close() {
                iterable.close();
            }

            @Override
            public Iterator<Entry<String, V>> iterator() {
                final Iterator<Entry<String, String>> iterator = iterable.iterator();


                return new Iterator<Entry<String, V>>() {
                    @Override
                    public boolean hasNext() {

                          return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, V> next() {
                        Entry<String, String> current;

                        current = iterator.next();
                        String key = current.key();
                        String json = current.value();
                        V value = toObject(json);

                        Entry<String, V> entry = new Entry<>(key, value);

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

    /**
     * Loads all of the key/values from the database.
     * @return the key and the values
     */
    @Override
    public KeyValueIterable<String, V> loadAll() {

        final KeyValueIterable<String, String> iterable;

        iterable = store.loadAll();



        return new KeyValueIterable<String, V>(){
            @Override
            public void close() {
                iterable.close();
            }

            @Override
            public Iterator<Entry<String, V>> iterator() {
                final Iterator<Entry<String, String>> iterator = iterable.iterator();


                return new Iterator<Entry<String, V>>() {
                    Entry<String, V> current;
                    @Override
                    public boolean hasNext() {

                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, V> next() {
                        Entry<String, String> current;

                        current = iterator.next();
                        String key = current.key();
                        String json = current.value();
                        V value = toObject( json );

                        Entry<String, V> entry = new Entry<>(key, value);

                        this.current = entry;
                        return entry;
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

    /**
     * Get the value from the store.
     * @return the value
     */
    @Override
    public V load(String key) {
        String value = store.load(prepareKey(key));
        if (value==null) {
            return null;
        }
        return deserializer.parse(type, value);
    }

    @Override
    public Map<String, V> loadAllByKeys(Collection<String> keys) {

        Set<String> keySet = new TreeSet<>(keys);

        final Map<String, String> map = store.loadAllByKeys(keySet);
        final Map<String, V> results= new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            results.put(entry.getKey(), toObject(entry.getValue()));
        }

        return results;
    }

    private V toObject(String json) {
        return deserializer.parse(type, json);
    }

    /**
     * Close this database.
     */
    @Override
    public void close() {
        store.close();
    }

}
