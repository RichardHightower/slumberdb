package info.slumberdb;


public class SimpleJsonKeyValueStoreLevelDB<V> extends SimpleJsonKeyValueStore<V> {


    public SimpleJsonKeyValueStoreLevelDB(StringKeyValueStore store, Class<V> cls) {
        super(store, cls);
    }

    public SimpleJsonKeyValueStoreLevelDB(String keyPrefix, StringKeyValueStore store, Class<V> cls) {
        super(keyPrefix, store, cls);

    }

    public SimpleJsonKeyValueStoreLevelDB(String keyPrefix, String fileName, Class<V> cls) {

        super(keyPrefix, new SimpleStringKeyValueStoreLevelDB(fileName), cls);
    }


    public SimpleJsonKeyValueStoreLevelDB(String fileName, Class<V> cls) {

        super(new SimpleStringKeyValueStoreLevelDB(fileName), cls);
    }

}
