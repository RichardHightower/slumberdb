package info.slumberdb;



/**
 * This is a marker interface of sorts for serialized JSON stores.
 * The main implementation will be Boon JSON serialization.
 */
public interface JsonKeyValueStore<K, V> extends KeyValueStore<K, V>{
}
