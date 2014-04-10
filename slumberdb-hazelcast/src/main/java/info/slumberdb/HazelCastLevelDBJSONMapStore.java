package info.slumberdb;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Richard on 4/7/14.
 */
public class HazelCastLevelDBJSONMapStore<V> implements MapStore<String, V> {

    final String fileName;
    final Class<V> cls;


    public HazelCastLevelDBJSONMapStore(String fileName, Class<V> cls) {
        this.fileName = fileName;
        this.cls = cls;
    }


    SimpleJsonKeyValueStoreLevelDB<V> store() {
        return new SimpleJsonKeyValueStoreLevelDB(this.fileName, this.cls);
    }

    @Override
    public void store(String key, V value) {
        final SimpleJsonKeyValueStoreLevelDB<V> store = store();

        try {
            store.put(key, value);
        } finally {
            store.close();

        }
    }

    @Override
    public void storeAll(Map<String, V> map) {

        final SimpleJsonKeyValueStoreLevelDB<V> store = store();

        try {
            store.putAll(map);
        } finally {
            store.close();

        }

    }

    @Override
    public void delete(String key) {

        final SimpleJsonKeyValueStoreLevelDB<V> store = store();

        try {
            store.remove(key);
        } finally {
            store.close();

        }


    }

    @Override
    public void deleteAll(Collection<String> keys) {


        final SimpleJsonKeyValueStoreLevelDB<V> store = store();

        try {
            store.removeAll(keys);
        } finally {
            store.close();

        }

    }

    @Override
    public V load(String key) {
        final SimpleJsonKeyValueStoreLevelDB<V> store = store();

        try {
            return store.load(key);
        } finally {
            store.close();

        }

    }

    @Override
    public Map<String, V> loadAll(Collection<String> keys) {

        final SimpleJsonKeyValueStoreLevelDB<V> store = store();

        try {
            return store.loadAllByKeys(keys);
        } finally {
            store.close();

        }

    }


    @Override
    public Set<String> loadAllKeys() {

        final SimpleJsonKeyValueStoreLevelDB<V> store = store();

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
