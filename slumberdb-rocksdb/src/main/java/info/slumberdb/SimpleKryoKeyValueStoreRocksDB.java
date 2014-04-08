package info.slumberdb;

import org.iq80.leveldb.Options;

import java.io.Serializable;

/**
 * Created by Richard on 4/8/14.
 */
public class SimpleKryoKeyValueStoreRocksDB  <T extends Serializable> extends SimpleKyroKeyValueStore <T> {


    public SimpleKryoKeyValueStoreRocksDB(String fileName, Options options, boolean log, Class<T> type) {
        super(new RocksDBKeyValueStore(fileName, options, log), type);
    }


    public SimpleKryoKeyValueStoreRocksDB(String fileName, Class<T> type) {
        super(new RocksDBKeyValueStore(fileName), type);
    }
}
