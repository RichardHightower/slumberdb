package info.slumberdb;

/**
 * A simple key value store always uses a string for the key.
 * @param <V> type of object
 */
public interface SimpleKeyValueStore <V> extends KeyValueStore <String, V>{
}
