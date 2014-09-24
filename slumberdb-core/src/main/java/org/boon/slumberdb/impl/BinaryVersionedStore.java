package org.boon.slumberdb.impl;

import org.boon.slumberdb.*;
import org.boon.slumberdb.entries.UpdateStatus;
import org.boon.slumberdb.entries.VersionKey;
import org.boon.slumberdb.entries.VersionedEntry;
import org.boon.slumberdb.spi.BaseVersionedStorage;

import java.util.Collection;
import java.util.Map;

/**
 * Created by Richard on 9/23/14.
 */
public class BinaryVersionedStore  implements KeyValueStoreWithVersion<String, byte[], VersionedEntry<String, byte[]>> {

    private final BaseVersionedStorage baseVersionedStorage;

    public BinaryVersionedStore(BaseVersionedStorage baseVersionedStorage) {
        this.baseVersionedStorage = baseVersionedStorage;
    }

    @Override
    public void put(String key, VersionedEntry<String, byte[]> value) {
            baseVersionedStorage.put(key, value);
    }

    @Override
    public void putAll(Map<String, VersionedEntry<String, byte[]>> values) {

        baseVersionedStorage.putAll(values);
    }

    @Override
    public void removeAll(Iterable<String> keys) {

        baseVersionedStorage.removeAll(keys);
    }

    @Override
    public void remove(String key) {

        baseVersionedStorage.remove(key);
    }

    @Override
    public KeyValueIterable<String, VersionedEntry<String, byte[]>> search(String startKey) {
        return baseVersionedStorage.search(startKey);
    }

    @Override
    public KeyValueIterable<String, VersionedEntry<String, byte[]>> loadAll() {
        return baseVersionedStorage.loadAll();
    }

    @Override
    public Collection<String> loadAllKeys() {
        return baseVersionedStorage.loadAllKeys();
    }

    @Override
    public VersionedEntry<String, byte[]> load(String key) {
        return baseVersionedStorage.load(key);
    }

    @Override
    public Map<String, VersionedEntry<String, byte[]>> loadAllByKeys(Collection<String> keys) {
        return baseVersionedStorage.loadAllByKeys(keys);
    }

    @Override
    public void close() {
        baseVersionedStorage.close();
    }

    @Override
    public boolean isOpen() {
        return baseVersionedStorage.isOpen();
    }

    @Override
    public boolean isClosed() {
        return baseVersionedStorage.isClosed();
    }

    @Override
    public VersionKey loadVersion(String key) {

        return baseVersionedStorage.loadVersion();
    }

    public final static UpdateStatus SUCCESS = new UpdateStatus(true, null);

    @Override
    public UpdateStatus put(String key, long version, VersionedEntry<String, byte[]> value) {

        final VersionKey versionKey = loadVersion(key);

        if (version > versionKey.version()) {

            VersionedEntry entry = new VersionedEntry<String, byte[]>();
            this.put(key, entry);

            return SUCCESS;
        } else {
            return new UpdateStatus(versionKey);
        }


    }

    @Override
    public UpdateStatus put(String key, long version, long updatedTime, VersionedEntry<String, byte[]> value) {

        final VersionKey versionKey = loadVersion(key);

        if (version > versionKey.version() && updatedTime > versionKey.updatedOn()) {

            VersionedEntry entry = new VersionedEntry<String, byte[]>();
            this.put(key, entry);

            return SUCCESS;
        } else {
            return new UpdateStatus(versionKey);
        }


    }

    @Override
    public UpdateStatus put(VersionKey key, VersionedEntry entry) {

        final VersionKey versionKey = loadVersion(key.key());

        if (key.equals(versionKey)) {
            this.put(key.key(), entry);
            return SUCCESS;
        } else {
            return new UpdateStatus(versionKey);
        }
    }

}
