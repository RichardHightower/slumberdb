package org.boon.slumberdb.entries;

/**
 * Created by Richard on 9/23/14.
 */
public class VersionKey {


    private final String key;
    private final long updateTimestamp;
    private final long version;
    private final int size;

    public VersionKey(String key, long version, long updateTimestamp, int size) {
        this.key = key;
        this.updateTimestamp = updateTimestamp;
        this.version = version;
        this.size = size;
    }


    public String key() {
        return key;
    }


    public long updatedOn() {
        return updateTimestamp;
    }

    public long version() {
        return version;
    }


    public int size() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VersionKey)) return false;

        VersionKey that = (VersionKey) o;

        if (updateTimestamp != that.updateTimestamp) return false;
        if (version != that.version) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;

        if (size!=-1 && that.size!=-1) {
            if (size != that.size) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (int) (updateTimestamp ^ (updateTimestamp >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));

        if (size!=-1)
        result = 31 * result + size;
        return result;
    }
}
