package info.slumberdb;

import org.iq80.leveldb.Options;

/**
 * Created by Richard on 4/8/14.
 */
public class SimpleStringKeyValueStoreRocksDB extends SimpleStringKeyValueStore {

    public SimpleStringKeyValueStoreRocksDB(String fileName, Options options) {
        store = new RocksDBKeyValueStore(fileName, options, false);
    }

    public SimpleStringKeyValueStoreRocksDB(String fileName, Options options, boolean log) {
        store = new RocksDBKeyValueStore(fileName, options, log);
    }

    public SimpleStringKeyValueStoreRocksDB(String fileName) {
        store = new RocksDBKeyValueStore(fileName);
    }


}

