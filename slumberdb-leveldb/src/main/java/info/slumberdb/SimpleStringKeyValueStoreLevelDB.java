package info.slumberdb;


import org.iq80.leveldb.Options;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.boon.Str.str;
import static org.boon.primitive.Byt.bytes;


public class SimpleStringKeyValueStoreLevelDB extends SimpleStringKeyValueStore {

    public SimpleStringKeyValueStoreLevelDB(String fileName, Options options) {
        store = new LevelDBKeyValueStore(fileName, options, false);
    }

    public SimpleStringKeyValueStoreLevelDB(String fileName, Options options, boolean log) {
        store = new LevelDBKeyValueStore(fileName, options, log);
    }

    public SimpleStringKeyValueStoreLevelDB(String fileName) {
        store = new LevelDBKeyValueStore(fileName);
    }


}

