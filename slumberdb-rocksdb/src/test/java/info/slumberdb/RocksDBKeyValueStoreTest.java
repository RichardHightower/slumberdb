package info.slumberdb;

import org.boon.Str;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Created by Richard on 4/4/14.
 */
public class RocksDBKeyValueStoreTest  {

    RocksDBKeyValueStore store;

    @Before
    public void setup() {

        File file = new File("target/test-data");
        file = file.getAbsoluteFile();
        file.mkdirs();
        file = new File(file, "bytes.dat");
        store = new RocksDBKeyValueStore(file.toString(), null, true);

    }

    @Test
    public void test() {
        store.put("hello".getBytes(StandardCharsets.UTF_8),
                "world".getBytes(StandardCharsets.UTF_8)
        );

        byte[] world = store.get("hello".getBytes(StandardCharsets.UTF_8));
        Str.equalsOrDie("world", new String(world, StandardCharsets.UTF_8));
    }


}