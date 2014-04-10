package info.slumberdb;

import com.esotericsoftware.kryo.Kryo;
import info.slumberdb.base.BaseStringBinaryKeyValueStore;
import info.slumberdb.serialization.KryoByteArrayToObjectConverter;
import info.slumberdb.serialization.KryoObjectToByteArrayConverter;

import java.io.Serializable;

/**
 * Created by Richard on 4/5/14.
 */
public class SimpleKryoKeyValueStoreMySQL<V extends Serializable> extends BaseStringBinaryKeyValueStore<String, V> implements SerializedJavaKeyValueStore<String, V> {

    /**
     * Kryo valueObjectConverter/valueSerializer
     */
    private final Kryo kryo = new Kryo();


    public SimpleKryoKeyValueStoreMySQL(String url, String userName, String password, String table, Class<V> type) {
        super(new SimpleStringBinaryKeyValueStoreMySQL(url, userName, password, table));
        this.valueObjectConverter = new KryoByteArrayToObjectConverter<>(kryo, type);
        this.valueSerializer = new KryoObjectToByteArrayConverter<>(kryo, type);
    }


}
