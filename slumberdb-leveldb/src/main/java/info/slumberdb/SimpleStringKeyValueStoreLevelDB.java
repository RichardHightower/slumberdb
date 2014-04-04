package info.slumberdb;


import org.iq80.leveldb.Options;



public class SimpleStringKeyValueStoreLevelDB extends SimpleStringKeyValueStore {

    public SimpleStringKeyValueStoreLevelDB(String fileName, Options options) {
        store = new LevelDBKeyValueStore(fileName, options, false);
    }

    public SimpleStringKeyValueStoreLevelDB(String fileName, Options options, boolean log) {
        store = new LevelDBKeyValueStore(fileName, options, log);
    }

    public SimpleStringKeyValueStoreLevelDB(String fileName) {
        store = new LevelDBKeyValueStore(fileName);
    }


}

