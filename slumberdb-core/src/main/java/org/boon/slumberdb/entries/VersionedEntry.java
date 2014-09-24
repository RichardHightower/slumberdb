package org.boon.slumberdb.entries;

import java.util.Map;

/**
 * Created by Richard on 9/23/14.
 */
public class VersionedEntry<K, V> extends Entry<K, V> {

    private long createTimestamp;
    private long updateTimestamp;
    private long version;

    public VersionedEntry(){
        
    }

    public VersionedEntry(Map.Entry<K, V> entry) {
        super(entry);
    }

    public VersionedEntry(K k, V v) {
        super(k, v);
    }


    public long updatedOn() {
        return updateTimestamp;
    }
    public long version() {
        return version;
    }
    public long createdOn() {
        return createTimestamp;
    }


    public VersionedEntry setVersion(long version) {
        this.version = version;
        return this;
    }

    public VersionedEntry setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
        return this;
    }

    public VersionedEntry setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
        return this;
    }
}
