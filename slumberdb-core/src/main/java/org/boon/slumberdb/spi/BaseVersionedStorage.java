package org.boon.slumberdb.spi;

import org.boon.slumberdb.KeyValueIterable;
import org.boon.slumberdb.entries.VersionKey;
import org.boon.slumberdb.entries.VersionedEntry;

import java.util.Collection;
import java.util.Map;

/**
 * Created by Richard on 9/23/14.
 */
public interface BaseVersionedStorage {
    long totalConnectionOpen();

    long totalClosedConnections();

    long totalErrors();

    void removeAll(Iterable<String> keys);

    void remove(String key);

    KeyValueIterable<String, VersionedEntry<String, byte[]>> search(String startKey);

    void close();

    Collection<String> loadAllKeys();

    VersionedEntry<String, byte[]> load(String key);

    void put(String key, VersionedEntry<String, byte[]> entry);

    void putAll(Map<String, VersionedEntry<String, byte[]>> values);

    Map<String, VersionedEntry<String, byte[]>> loadAllByKeys(Collection<String> keys);

    KeyValueIterable<String, VersionedEntry<String, byte[]>> loadAll();

    boolean isOpen();

    boolean isClosed();

    VersionKey loadVersion();
}
