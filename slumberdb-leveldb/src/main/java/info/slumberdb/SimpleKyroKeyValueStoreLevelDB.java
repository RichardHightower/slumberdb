package info.slumberdb;

import org.iq80.leveldb.Options;

import java.io.Serializable;

/**
 * Created by Richard on 4/5/14.
 */
public class SimpleKyroKeyValueStoreLevelDB <T extends Serializable> extends SimpleKyroKeyValueStore <T> {


    public SimpleKyroKeyValueStoreLevelDB(String fileName, Options options, boolean log, Class<T> type) {
        super(new LevelDBKeyValueStore(fileName, options, log), type);
    }


    public SimpleKyroKeyValueStoreLevelDB(String fileName, Class<T> type) {
        super(new LevelDBKeyValueStore(fileName), type);
    }
}
