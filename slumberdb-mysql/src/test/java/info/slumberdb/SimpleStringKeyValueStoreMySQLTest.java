package info.slumberdb;

import org.boon.Maps;
import org.boon.Str;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.boon.Boon.puts;
import static org.boon.Exceptions.die;
import static org.boon.Ok.okOrDie;


public class SimpleStringKeyValueStoreMySQLTest {
    private SimpleStringKeyValueStoreMySQL store;
    String url = "jdbc:mysql://localhost:3306/slumberdb";
    String userName = "slumber";
    String password = "slumber1234";
    String table = "string-test";


    boolean ok;

    @Before
    public void setup() {

        store = new SimpleStringKeyValueStoreMySQL(url, userName, password, table);

    }

    @After
    public void close() {


        store.close();
    }



    @Test
    public void test() {
        store.put("hello",
                "world"
        );

        String world = store.get("hello");
        Str.equalsOrDie("world", world);

    }


    @Test
    public void testBulkPut() {

        Map<String, String> map = Maps.map("hello1", "hello1",
                "hello2", "hello2");


        store.putAll(map);


        String value ;

        value =        store.get("hello1");
        Str.equalsOrDie("hello1", value);


        value =        store.get("hello2");
        Str.equalsOrDie("hello2", value);


        store.remove("hello2");
        value =        store.get("hello2");
        okOrDie(value == null);
    }



    @Test
    public void testBulkRemove() {

        Map<String, String> map = Maps.map("hello1", "hello1",
                "hello2", "hello2");


        store.putAll(map);
        store.put("somethingElse", "1");


        String value ;

        value =        store.get("hello1");
        Str.equalsOrDie("hello1", value);


        value =        store.get("hello2");
        Str.equalsOrDie("hello2", value);


        store.removeAll(map.keySet());



        value =        store.get("hello1");

        ok = value == null || die();

        value =        store.get("hello2");


        ok = value == null || die();


        Str.equalsOrDie("1", store.get("somethingElse"));




    }



    @Test
    public void testSearch() {
        for (int index=0; index< 100; index++) {
            store.put("key" + index, "value" + index);
        }

        KeyValueIterable<String, String> entries = store.search("key50");
        for (Entry<String, String> entry : entries) {
            puts (entry.key(), entry.value());
        }

        entries.close();
    }


    @Test
    public void testSearch2() {
        for (int index=0; index< 100; index++) {
            store.put("key" + index, "value" + index);
        }

        KeyValueIterable<String, String> entries = store.search("key50");
        for (Entry<String, String> entry : entries) {
            puts (entry.key(), entry.value());
        }

        entries.close();
    }






}
