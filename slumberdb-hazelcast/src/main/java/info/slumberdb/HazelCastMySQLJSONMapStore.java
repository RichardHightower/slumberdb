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

    final String url;
    final String userName;
    final String password;
    final String table;
    final Class<V> type;

    public HazelCastMySQLJSONMapStore(String url, String userName, String password, String table, Class<V> type) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.table = table;
        this.type = type;
    }


    private SimpleJsonKeyValueStoreMySQL<V> store() {
        SimpleJsonKeyValueStoreMySQL<V> store;
        store = new SimpleJsonKeyValueStoreMySQL<>(url, userName, password, table, type);
        return store;
    }



    @Override
    public void store(String key, V value) {
        final SimpleJsonKeyValueStoreMySQL<V> store = store();

        try {
            store.put(key, value);
        } finally {
            store.close();

        }
    }

    @Override
    public void storeAll(Map<String, V> map) {

        final SimpleJsonKeyValueStoreMySQL<V> store = store();

        try {
            store.putAll(map);
        } finally {
            store.close();

        }

    }

    @Override
    public void delete(String key) {

        final SimpleJsonKeyValueStoreMySQL<V> store = store();

        try {
            store.remove(key);
        } finally {
            store.close();

        }


    }

    @Override
    public void deleteAll(Collection<String> keys) {


        final SimpleJsonKeyValueStoreMySQL<V> store = store();

        try {
            store.removeAll(keys);
        } finally {
            store.close();

        }

    }

    @Override
    public V load(String key) {
        final SimpleJsonKeyValueStoreMySQL<V> store = store();

        try {
            return store.load(key);
        } finally {
            store.close();

        }

    }

    @Override
    public Map<String, V> loadAll(Collection<String> keys) {

        final SimpleJsonKeyValueStoreMySQL<V> store = store();

        try {
            return store.loadAllByKeys(keys);
        } finally {
            store.close();

        }

    }


    @Override
    public Set<String> loadAllKeys() {

        final SimpleJsonKeyValueStoreMySQL<V> store = store();

        try {
            final Collection<String> keys = store.loadAllKeys();
            if (keys instanceof Set) {
                return (Set) keys;
            } else {
                return new LinkedHashSet<>(keys);
            }
        } finally {
            store.close();

        }


    }
}
