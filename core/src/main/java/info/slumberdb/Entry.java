package info.slumberdb;


import java.io.Serializable;

/**
 * Represents an entry in the database.
 * @param <K> key
 * @param <V> value
 */
public class Entry <K, V> implements Serializable {
    /** Key of item. */
    private final K key;

    /** Value of item.*/
    private final V value;

    /** Create an entry. */
    public Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }


    /**
     * Used for serialization.
     */
    public Entry() {
        key = null;
        value = null;

    }

    /** Get the key. */
    public K key() {
        return key;
    }

    /** Get the value. */
    public V value() {
        return value;
    }
}
