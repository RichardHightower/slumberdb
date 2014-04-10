package info.slumberdb;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Richard on 4/7/14.
 */
public class HazelCastMySQLJSONMapStore<V> implements MapStore<String, V> {


    private SimpleJsonKeyValueStoreMySQL<V> store;

    public HazelCastMySQLJSONMapStore(String url, String userName, String password, String table, Class<V> type) {

        this.store = new SimpleJsonKeyValueStoreMySQL<>(url, userName, password, table, type);
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
        return store.load(key);
    }

    @Override
    public Map<String, V> loadAll(Collection<String> keys) {

        return store.loadAllByKeys(keys);
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
