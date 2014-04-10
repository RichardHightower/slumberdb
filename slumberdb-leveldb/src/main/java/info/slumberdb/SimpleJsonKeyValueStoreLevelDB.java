package info.slumberdb;


public class SimpleJsonKeyValueStoreLevelDB<V> extends SimpleJsonKeyValueStore<V> {


    public SimpleJsonKeyValueStoreLevelDB(String fileName, Class<V> cls) {

        super(new LevelDBKeyValueStore(fileName), cls);
    }

}
