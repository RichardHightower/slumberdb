package info.slumberdb;

import java.io.Closeable;
import java.io.Flushable;
import java.util.Map;

/**
 * A map like interface that represents a key value store.
 * This is not a map. KeyValueStore can be closed and flushed.
 * <p>
 * When you <code>put(...)</code> a value,
 * it may return async so put does not return a value.
 * </p>
 *
 */
public interface KeyValueStore <K, V> extends Closeable, Flushable{

    /**
     * Put a key
     * @param key  key
     * @param value value
     */
    void put(K key, V value);



    /**
     * Put all values.
     */
    void putAll(Map<K, V> values);

    /**
     * Remove all values
     */
    void removeAll(Iterable<K> keys);


    /**
     * Update values.
     * Allows batching updates.
     */
    void updateAll(Iterable<CrudOperation> updates);

    /** Remove a single key. */
    void remove(K key);



    /**
     * Search.
     */
    KeyValueIterable<K, V> search(K startKey);


    /**
     * Load All Values.
     * This is good for in-memory caches that have some keys that
     * are persistent.
     */
    KeyValueIterable<K, V> loadAll();

    /*
     * Get a value from the store.
     */
    V get(K key);


    /*
     * Close the connection to the database.
     */
    void close();


    /*
     * Flush any outstanding requests.
     */
    void flush();

}
