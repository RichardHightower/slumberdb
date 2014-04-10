package info.slumberdb;

/**
 * Created by Richard on 4/8/14.
 */
public class SimpleJsonKeyValueStoreRocksDB<V> extends SimpleJsonKeyValueStore<V> {


    public SimpleJsonKeyValueStoreRocksDB(String fileName, Class<V> cls) {

        super(new RocksDBKeyValueStore(fileName), cls);
    }

}
