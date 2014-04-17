package info.slumberdb;

import com.hazelcast.core.MapStore;
import info.slumberdb.config.ConfigUtils;
import info.slumberdb.config.DatabaseConfig;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Richard on 4/7/14.
 */
public class HazelCastMySQLJSONMapStore<V> implements MapStore<String, V> {

    private final String url;
    private final String userName;
    private final String password;
    private final String table;
    private final Class<V> type;

    private static final String SLUMBERDB_HAZELCAST_CONFIG_PATH =
            System.getProperty("SLUMBERDB_HAZEL_CAST_CONFIG_PATH",
                    "/conf/slumberdb/hazelcast/mapstore.json");

    public HazelCastMySQLJSONMapStore() {
        this(SLUMBERDB_HAZELCAST_CONFIG_PATH);
    }

    public HazelCastMySQLJSONMapStore(String path) {
        this (ConfigUtils.readConfig(path, DatabaseConfig.class));
    }

    public HazelCastMySQLJSONMapStore(DatabaseConfig databaseConfig) {
        this.url = databaseConfig.getUrl();
        this.userName = databaseConfig.getUserName();
        this.password = databaseConfig.getPassword();
        this.table = databaseConfig.getTableName();
        this.type = (Class)  databaseConfig.getComponentClass();
    }

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
