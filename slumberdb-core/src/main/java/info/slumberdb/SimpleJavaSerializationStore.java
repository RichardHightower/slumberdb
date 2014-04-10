package info.slumberdb;

import info.slumberdb.base.BaseSimpleSerializationKeyValueStore;
import info.slumberdb.serialization.ByteArrayToStringConverter;
import info.slumberdb.serialization.JavaDeserializerBytes;
import info.slumberdb.serialization.JavaSerializerBytes;
import info.slumberdb.serialization.StringToByteArrayConverter;

import java.io.Serializable;

/**
 * This is done mostly to benchmark it against Kryo to show how awesome Kryo is.
 */
public class SimpleJavaSerializationStore<V extends Serializable> extends BaseSimpleSerializationKeyValueStore<String, V> implements SerializedJavaKeyValueStore<String, V> {

    /**
     * @param store store
     */
    public SimpleJavaSerializationStore(final KeyValueStore<byte[], byte[]> store
    ) {
        super(store);
        this.valueObjectConverter = new JavaDeserializerBytes();
        this.valueToByteArrayConverter = new JavaSerializerBytes();
        this.keyObjectConverter = new ByteArrayToStringConverter();
        this.keyToByteArrayConverter = new StringToByteArrayConverter();

    }

}
