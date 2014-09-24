package org.boon.slumberdb.entries;

/**
 * Created by Richard on 9/23/14.
 */
public class VersionedKeyValuePut <K, V> {

    private final VersionKey key;
    private final Entry<K, V> value;

    public VersionedKeyValuePut(VersionKey key, Entry<K, V> value) {
        this.key = key;
        this.value = value;
    }
}
