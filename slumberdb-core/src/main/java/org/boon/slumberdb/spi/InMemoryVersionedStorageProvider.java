package org.boon.slumberdb.spi;

import org.boon.collections.LazyMap;
import org.boon.slumberdb.entries.Entry;
import org.boon.slumberdb.KeyValueIterable;
import org.boon.slumberdb.entries.VersionKey;
import org.boon.slumberdb.entries.VersionedEntry;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by Richard on 9/23/14.
 */
public class InMemoryVersionedStorageProvider implements VersionedStorageProvider {

    private static final int PADDING = 1024;

    private final ConcurrentNavigableMap<String, ByteBuffer> searchMap = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, ByteBuffer> map = new ConcurrentHashMap<>(10_000);

    @Override
    public long totalConnectionOpen() {
        return 1;
    }

    @Override
    public long totalClosedConnections() {
        return 0;
    }

    @Override
    public long totalErrors() {
        return 0;
    }

    @Override
    public void removeAll(Iterable<String> keys) {

        for (String key : keys) {
            map.remove(key);
            searchMap.remove(key);
        }
    }

    @Override
    public void remove(String key) {

        map.remove(key);
        searchMap.remove(key);

    }

    @Override
    public KeyValueIterable<String, VersionedEntry<String, byte[]>> search(String startKey) {


        final ConcurrentNavigableMap<String,ByteBuffer> subMap = searchMap.tailMap(startKey);
        final Iterator<Map.Entry<String, ByteBuffer>> iterator = subMap.entrySet().iterator();

        return new KeyValueIterable<String, VersionedEntry<String, byte[]>>() {
            @Override
            public void close() {

            }

            @Override
            public Iterator<Entry<String, VersionedEntry<String, byte[]>>> iterator() {
                return new Iterator<Entry<String, VersionedEntry<String, byte[]>>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, VersionedEntry<String, byte[]>> next() {
                        final Map.Entry<String, ByteBuffer> entry = iterator.next();


                        final ByteBuffer byteBuffer = entry.getValue();
                        return new Entry<>(entry.getKey(), readEntry(entry.getKey(), byteBuffer));

                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }

    private VersionedEntry<String, byte[]> readEntry(final String key, ByteBuffer byteBuffer) {
        VersionedEntry<String, byte[]> versionedEntry = new VersionedEntry<>(key, null);


        byteBuffer.rewind();
        versionedEntry.setVersion(byteBuffer.getLong());
        versionedEntry.setCreateTimestamp(byteBuffer.getLong());
        versionedEntry.setUpdateTimestamp(byteBuffer.getLong());
        final int size = byteBuffer.getInt();
        byte[] bytes = new byte[size];
        byteBuffer.get(bytes);
        versionedEntry.setValue(bytes);

        return versionedEntry;
    }

    private void writeEntry(
            VersionedEntry<String, byte[]> versionedEntry, ByteBuffer byteBuffer) {



        byteBuffer.rewind();
        byteBuffer.putLong(versionedEntry.version());
        byteBuffer.putLong(versionedEntry.createdOn());
        byteBuffer.putLong(versionedEntry.updatedOn());
        byteBuffer.putInt(versionedEntry.getValue().length);
        byteBuffer.put(versionedEntry.getValue());

    }

    @Override
    public void close() {

    }

    @Override
    public Collection<String> loadAllKeys() {
        return searchMap.keySet();
    }

    @Override
    public VersionedEntry<String, byte[]> load(String key) {
        ByteBuffer byteBuffer = map.get(key);
        return readEntry(key, byteBuffer);
    }

    @Override
    public void put(String key, VersionedEntry<String, byte[]> entry) {

        ByteBuffer byteBuffer = map.get(key);
        if (byteBuffer==null) {
            byteBuffer = ByteBuffer.allocate(255 + entry.getValue().length);
        }


        if (byteBuffer.capacity() < (entry.getValue().length + 32)) {
            byteBuffer = ByteBuffer.allocateDirect(32 + PADDING + entry.getValue().length);
        }

        writeEntry(entry, byteBuffer);

        map.put(key, byteBuffer);
        searchMap.put(key, byteBuffer);

    }

    @Override
    public void putAll(Map<String, VersionedEntry<String, byte[]>> values) {


        for (Map.Entry<String, VersionedEntry<String, byte[]>> entry : values.entrySet()) {

            put(entry.getKey(), entry.getValue());
        }

    }

    @Override
    public Map<String, VersionedEntry<String, byte[]>> loadAllByKeys(Collection<String> keys) {
        LazyMap outputMap = new LazyMap(keys.size());

        for (String key : keys) {

            outputMap.put(key, load(key));
        }

        return (Map<String, VersionedEntry<String, byte[]>>) (Object) outputMap;
    }

    @Override
    public KeyValueIterable<String, VersionedEntry<String, byte[]>> loadAll() {

        final Iterator<Map.Entry<String, ByteBuffer>> iterator = searchMap.entrySet().iterator();

        return new KeyValueIterable<String, VersionedEntry<String, byte[]>>() {
            @Override
            public void close() {

            }

            @Override
            public Iterator<Entry<String, VersionedEntry<String, byte[]>>> iterator() {
                return new Iterator<Entry<String, VersionedEntry<String, byte[]>>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String, VersionedEntry<String, byte[]>> next() {
                        final Map.Entry<String, ByteBuffer> entry = iterator.next();


                        final ByteBuffer byteBuffer = entry.getValue();
                        return new Entry<>(entry.getKey(), readEntry(entry.getKey(), byteBuffer));

                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };

    }

    @Override
    public List<VersionKey> loadAllVersionInfoByKeys(Collection<String> keys) {

        List<VersionKey> versionKeys = new ArrayList<>(keys.size());

        for (String key : keys) {

            final ByteBuffer byteBuffer = map.get(key);
            VersionKey versionKey = readVersion(key, byteBuffer);
            versionKeys.add(versionKey);

        }
        return versionKeys;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public VersionKey loadVersion(String key) {

        final ByteBuffer byteBuffer = map.get(key);
        VersionKey versionKey = readVersion(key, byteBuffer);
        return versionKey;
    }


    private VersionKey readVersion(final String key, ByteBuffer byteBuffer) {

        if (byteBuffer==null) {
            return VersionKey.notFound(key);
        }

        byteBuffer.rewind();
        long version = byteBuffer.getLong();
        long createTime = byteBuffer.getLong();
        long updateTimestamp = byteBuffer.getLong();
        final int size = byteBuffer.getInt();

        return new VersionKey(key, version, updateTimestamp,createTime, size);

    }
}
