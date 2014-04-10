package info.slumberdb;

import info.slumberdb.base.BaseStringStringKeyValueStore;
import info.slumberdb.serialization.JsonDeserializer;
import info.slumberdb.serialization.JsonSerializer;

/**
 * Created by Richard on 4/4/14.
 */
public class SimpleJsonKeyValueStoreMySQL<V> extends BaseStringStringKeyValueStore<String, V> {

    public SimpleJsonKeyValueStoreMySQL(String url, String userName, String password, String table, Class<V> cls) {
        super(new SimpleStringKeyValueStoreMySQL(url, userName, password, table));


        this.valueObjectConverter = new JsonDeserializer<>(cls);
        this.valueSerializer = new JsonSerializer<>(cls);
    }

}