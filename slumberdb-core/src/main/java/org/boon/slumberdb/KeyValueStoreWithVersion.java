package org.boon.slumberdb;

import org.boon.slumberdb.entries.UpdateStatus;
import org.boon.slumberdb.entries.VersionKey;
import org.boon.slumberdb.entries.VersionedEntry;

/**
 * Created by Richard on 9/23/14.
 */
public interface KeyValueStoreWithVersion <K, O, V extends VersionedEntry<K, O>> extends KeyValueStore<K, V>{


    VersionKey loadVersion(String key);

    UpdateStatus put(String key, long version, V value);


    UpdateStatus put(String key, long version, long updatedTime, V value);

    UpdateStatus put(VersionKey key, VersionedEntry entry);


}
