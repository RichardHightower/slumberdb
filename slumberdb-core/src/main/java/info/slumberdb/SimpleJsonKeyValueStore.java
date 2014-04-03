package info.slumberdb;

import org.boon.Str;
import org.boon.json.JsonParserAndMapper;
import org.boon.json.JsonParserFactory;
import org.boon.json.serializers.impl.JsonSimpleSerializerImpl;

import java.util.*;


public class SimpleJsonKeyValueStore<V> implements JsonKeyValueStore<String, V> {

    protected final Class<V> type;
    protected JsonSimpleSerializerImpl serializer = new JsonSimpleSerializerImpl();
    protected JsonParserAndMapper deserializer = new JsonParserFactory().create();

    protected StringKeyValueStore store;

    protected final String keyPrefix;



    public SimpleJsonKeyValueStore(StringKeyValueStore store, Class<V> cls) {
        this(null, store, cls);
    }

    public SimpleJsonKeyValueStore(String keyPrefix, StringKeyValueStore store, Class<V> cls) {
        type = cls;
        this.store = store;
        this.keyPrefix = keyPrefix;

    }

    private String toJson(V value) {
        return serializer.serialize(value).toString();
    }

    @Override
    public void put(String key, V value) {

        store.put(prepareKey(key), toJson(value));

    }

    private String prepareKey(String key) {
        if (keyPrefix!=null) {
            return Str.add(keyPrefix, ".", key);
        } else {
            return key;
        }
    }

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

    @Override
    public void removeAll(Iterable<String> keys) {

        if (keyPrefix==null) {

            List keysPrepared = new ArrayList();

            for (String key : keys) {
                keysPrepared.add(prepareKey(key));
            }
            store.removeAll(keys);

        } else {
            store.removeAll(keys);

        }

    }

    @Override
    public void updateAll(Iterable<CrudOperation> updates) {

    }

    @Override
    public void remove(String key) {

    }


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
                        V value = deserializer.parse(type, json);

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
                        V value = deserializer.parse(type, json);

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
    public V get(String key) {
        String value = store.get(prepareKey(key));
        if (value==null) {
            return null;
        }
        return deserializer.parse(type, value);
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public void flush() {
        store.flush();
    }
}
