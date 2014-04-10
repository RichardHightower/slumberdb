package info.slumberdb;

import info.slumberdb.base.BaseStringBinaryKeyValueStore;
import info.slumberdb.serialization.JavaDeserializerBytes;
import info.slumberdb.serialization.JavaSerializerBytes;

import java.io.Serializable;

/**
 * Created by Richard on 4/4/14.
 */
public class SimpleJavaSerializationKeyValueStoreMySQL<V extends Serializable> extends BaseStringBinaryKeyValueStore<String, V> implements SerializedJavaKeyValueStore<String, V> {


    public SimpleJavaSerializationKeyValueStoreMySQL(String url, String userName, String password, String table, Class<V> type) {
        super(new SimpleStringBinaryKeyValueStoreMySQL(url, userName, password, table));
        this.valueObjectConverter = new JavaDeserializerBytes<>();
        this.valueSerializer = new JavaSerializerBytes<>();
    }


}
