package info.slumberdb;

/**
 * Created by Richard on 4/8/14.
 */
public class SimpleJsonKeyValueStoreRocksDB <V> extends SimpleJsonKeyValueStore<V> {


    public SimpleJsonKeyValueStoreRocksDB(StringKeyValueStore store, Class<V> cls) {
        super(store, cls);
    }

    public SimpleJsonKeyValueStoreRocksDB(String keyPrefix, StringKeyValueStore store, Class<V> cls) {
        super(keyPrefix, store, cls);

    }

    public SimpleJsonKeyValueStoreRocksDB(String keyPrefix, String fileName, Class<V> cls) {

        super(keyPrefix, new SimpleStringKeyValueStoreRocksDB(fileName), cls);
    }


    public SimpleJsonKeyValueStoreRocksDB(String fileName, Class<V> cls) {

        super(new SimpleStringKeyValueStoreRocksDB(fileName), cls);
    }

}
