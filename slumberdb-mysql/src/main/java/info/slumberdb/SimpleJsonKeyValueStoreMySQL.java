package info.slumberdb;

/**
 * Created by Richard on 4/4/14.
 */
public class SimpleJsonKeyValueStoreMySQL <V> extends SimpleJsonKeyValueStore<V> {

    public SimpleJsonKeyValueStoreMySQL(String url, String userName, String password, String table, Class<V> cls) {
        super(new SimpleStringKeyValueStoreMySQL(url, userName, password, table), cls);
    }

}