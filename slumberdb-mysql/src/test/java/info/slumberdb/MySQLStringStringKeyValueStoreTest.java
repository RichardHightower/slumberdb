package info.slumberdb;

import org.boon.Str;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class MySQLStringStringKeyValueStoreTest {
    private MySQLStringStringKeyValueStore store;
    String url = "jdbc:mysql://localhost:3306/slumberdb";
    String userName = "slumber";
    String password = "slumber1234";
    String table = "string-test";


    boolean ok;

    @Before
    public void setup() {

        store = new MySQLStringStringKeyValueStore(url, userName, password, table);

    }



    @Test
    public void test() {
        store.put("hello",
                "world"
        );

        String world = store.get("hello");
        Str.equalsOrDie("world", world);

    }


    @After
    public void close() {
        store.close();
    }


}
