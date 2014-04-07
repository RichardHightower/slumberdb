package info.slumberdb;

import com.hazelcast.core.MapStore;

import java.util.*;

/**
 * Created by Richard on 4/7/14.
 */
public class HazelCastMySQLJSONMapStore<V> implements MapStore<String, V>{

    private String url;
    private String userName;
    private String password;
    private String table;
    private Class<V> type;

    private SimpleJsonKeyValueStoreMySQL<V> store;

    public HazelCastMySQLJSONMapStore(String url, String userName, String password, String table, Class<V> type) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.table = table;
        this.type = type;
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
        final KeyValueIterable<String, V> entries = store.loadAll();
        final Set<String> set = new HashSet<>();
        for (Entry<String, V> entry : entries) {
            set.add(entry.key());
        }
        return set;
    }
}
