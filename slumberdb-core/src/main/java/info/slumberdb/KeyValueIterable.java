package info.slumberdb;


import java.io.Closeable;

/**
 * Iterate over key / value store
 * @param <K> KEY
 * @param <V> VALUE
 */
public interface KeyValueIterable <K, V> extends Iterable<Entry<K, V>>, Closeable {

    public void close();
}
