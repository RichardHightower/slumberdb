package info.slumberdb;

import org.iq80.leveldb.Options;

/**
 * Created by Richard on 4/8/14.
 */
public class SimpleStringKeyValueStoreRocksDB extends SimpleStringKeyValueStore {

    public SimpleStringKeyValueStoreRocksDB(String fileName, Options options) {
        super(new RocksDBKeyValueStore(fileName, options, false));
    }

    public SimpleStringKeyValueStoreRocksDB(String fileName, Options options, boolean log) {
        super(new RocksDBKeyValueStore(fileName, options, log));
    }

    public SimpleStringKeyValueStoreRocksDB(String fileName) {
        super(new RocksDBKeyValueStore(fileName));
    }


}

