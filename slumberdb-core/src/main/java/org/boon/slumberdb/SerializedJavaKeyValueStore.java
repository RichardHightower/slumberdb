package org.boon.slumberdb;

/**
 * This is a marker interface of sorts for serialized java object stores.
 * The main implementation will be Kyro.
 */
public interface SerializedJavaKeyValueStore<K, V> extends KeyValueStore<K, V> {
}
