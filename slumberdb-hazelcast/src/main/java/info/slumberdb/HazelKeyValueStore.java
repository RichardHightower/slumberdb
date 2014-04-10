package info.slumberdb;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import org.boon.Str;

import java.io.Serializable;
import java.util.*;


/**
 * Created by Richard on 4/9/14.
 */
public class HazelKeyValueStore <K extends Serializable, V extends Serializable> implements KeyValueStore<K, V>{

    private final Config config;

    private final String mapName;

    private final boolean blockUpdate;


    private HazelcastInstance hazelcastInstance;

    private final String searchSQL;


    private final IMap<K, V> map;

    public HazelKeyValueStore (String mapName, Config config, boolean blockUpdate,
                               String keyPropertyName) {
        this.config = config;
        this.mapName = mapName;
        hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(config);
        map = hazelcastInstance.getMap(mapName);
        this.blockUpdate = blockUpdate;


        this.searchSQL = Str.add(keyPropertyName, " > ");

    }

    @Override
    public void put(K key, V value) {

        if (blockUpdate) {
            map.put(key, value);
        } else {
            map.putAsync(key, value);
        }

    }

    @Override
    public void putAll(Map<K, V> values) {

        if (blockUpdate) {
            map.putAll(values);

        } else {
            for (Map.Entry<K,V> entry : values.entrySet()) {
                map.putAsync(entry.getKey(), entry.getValue());
            }
        }

     }

    @Override
    public void removeAll(Iterable<K> keys) {

        if (blockUpdate) {
            for (K key : keys) {
                map.remove(key);
            }

        } else {
            for (K key : keys) {
                map.removeAsync(key);
            }
        }

    }

    @Override
    public void remove(K key) {

        if (blockUpdate) {
            map.remove(key);
        } else {
            map.removeAsync(key);
        }

    }


    String getSearchSQL(K startKey) {
        String value;
        if (startKey instanceof CharSequence) {
            value = Str.singleQuote(startKey.toString());
        } else {
            value = startKey.toString();
        }
        return Str.add(this.searchSQL, value);
    }

    @Override
    public KeyValueIterable<K, V> search(K startKey) {

        final String searchSQL1 = getSearchSQL(startKey);
        final Set<Map.Entry<K, V>> entries = map.entrySet(new SqlPredicate(searchSQL1));
        final Iterator<Map.Entry<K, V>> iterator = entries.iterator();

        return createKeyIteratorFromIterator(iterator);

    }

    @Override
    public KeyValueIterable<K, V> loadAll() {


        final Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();

        return createKeyIteratorFromIterator(iterator);
    }

    private KeyValueIterable<K, V> createKeyIteratorFromIterator(final Iterator<Map.Entry<K, V>> iterator) {
        return new KeyValueIterable<K, V>() {
            @Override
            public void close() {

            }

            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new Iterator<Entry<K, V>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        final Map.Entry<K, V> next = iterator.next();
                        return new Entry<>(next.getKey(), next.getValue());
                    }

                    @Override
                    public void remove() {

                    }
                };
            }
        };
    }

    @Override
    public Collection<K> loadAllKeys() {
        return map.keySet();
    }

    @Override
    public V load(K key) {
        return map.get(key);
    }

    @Override
    public Map<K, V> loadAllByKeys(Collection<K> keys) {

        Map<K, V> map = new LinkedHashMap<>(keys.size());

        final Set<Map.Entry<K, V>> entries = map.entrySet();
        for (Map.Entry<K, V> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    @Override
    public void close() {

    }
}
