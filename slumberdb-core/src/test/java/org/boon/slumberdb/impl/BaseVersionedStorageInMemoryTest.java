package org.boon.slumberdb.impl;

import org.boon.Boon;
import org.boon.slumberdb.entries.VersionedEntry;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.boon.Str.equalsOrDie;

/**
 * Created by Richard on 9/23/14.
 */
public class BaseVersionedStorageInMemoryTest {


    @Test
    public void test() throws Exception {

        BaseVersionedStorageInMemory baseStore = new BaseVersionedStorageInMemory();

        VersionedEntry<String, byte[]> versionedEntry = new VersionedEntry<>("Rick", null);

        versionedEntry.setValue("WAS HERE".getBytes(StandardCharsets.UTF_8));
        versionedEntry.setCreateTimestamp(99);
        versionedEntry.setVersion(1);
        versionedEntry.setUpdateTimestamp(66);

        baseStore.put("Rick", versionedEntry);

        final VersionedEntry<String, byte[]> rick = baseStore.load("Rick");

        equalsOrDie("Rick", rick.getKey());


        equalsOrDie("WAS HERE", new String(rick.getValue(), StandardCharsets.UTF_8));


        Boon.equalsOrDie(99L, rick.createdOn());

        Boon.equalsOrDie(66L, rick.updatedOn());

        Boon.equalsOrDie(1L, rick.version());




    }
}
