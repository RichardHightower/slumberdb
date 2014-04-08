package info.slumberdb;

import com.hazelcast.core.MapStore;

import java.util.*;

/**
 * Created by Richard on 4/7/14.
 */
public  class HazelCastLevelDBJSONMapStore <V> implements MapStore<String, V> {

    SimpleJsonKeyValueStoreLevelDB<V> store;

    public HazelCastLevelDBJSONMapStore(String keyPrefix, String fileName, Class<V> cls) {
        store = new SimpleJsonKeyValueStoreLevelDB(keyPrefix, fileName, cls);
    }

    public HazelCastLevelDBJSONMapStore(String fileName, Class<V> cls) {
        store = new SimpleJsonKeyValueStoreLevelDB(null, fileName, cls);
    }

    @Override
    public void store(String key, V value) {
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<String, V> map) {
        store.putAll(map);

    }

    @Override
    public void delete(String key) {
        store.remove(key);

    }

    @Override
    public void deleteAll(Collection<String> keys) {
        store.removeAll(keys);
    }

    @Override
    public V load(String key) {
        return store.get(key);
    }

    @Override
    public Map<String, V> loadAll(Collection<String> keys) {


        Map<String, V> map = new HashMap<>();

        for (String key : keys) {
            map.put(key, store.get(key));
        }

        return map;
    }


    @Override
    public Set<String> loadAllKeys() {
        final Collection<String> keys = store.loadAllKeys();
        if (keys instanceof Set) {
            return (Set) keys;
        } else {
            return new LinkedHashSet<>(keys);
        }
    }
}
