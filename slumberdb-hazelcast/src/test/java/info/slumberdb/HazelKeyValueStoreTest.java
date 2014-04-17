package info.slumberdb;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.boon.Maps;
import org.boon.Str;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import static org.boon.Boon.puts;
import static org.boon.Exceptions.die;

public class HazelKeyValueStoreTest {


    private HazelKeyValueStore<String, Employee> store;
    private boolean ok;



    static Config cfg = new Config();
    static HazelcastInstance instance;
    static {

        MapConfig mapCfg = new MapConfig();
        mapCfg.setName("employees-hazel-store-test");
        mapCfg.setAsyncBackupCount(0);
        MapStoreConfig mapStoreCfg = new MapStoreConfig();
        mapStoreCfg.setImplementation(new HazelCastMySQLJSONMapStore());
        mapStoreCfg.setEnabled(true);

        //mapCfg.addMapIndexConfig(new MapIndexConfig("id", true));
        //mapCfg.setMapStoreConfig(mapStoreCfg);


        cfg.addMapConfig(mapCfg);
        cfg.setInstanceName("hazelcast-test-kv-store");
        instance = Hazelcast.newHazelcastInstance(cfg);



    }


    @Before
    public void setup() {


        store = new HazelKeyValueStore("employees-hazel-store-test", cfg, false, "id");

    }

    @After
    public void close() {
    }

    @Test
    public void test() {
        store.put("123",
                new Employee("123", "Rick", "Hightower")
        );

        Employee employee = store.load("123");
        Str.equalsOrDie("Rick", employee.getFirstName());

        Str.equalsOrDie("Hightower", employee.getLastName());
    }

    @Test
    public void testBulkPut() {

        Map<String, Employee> map = Maps.map(

                "123", new Employee("123", "Rick", "Hightower"),
                "456", new Employee("456", "Paul", "Tabor"),
                "789", new Employee("789", "Jason", "Daniel")

        );


        store.putAll(map);


        Employee employee;


        employee = store.load("789");
        Str.equalsOrDie("Jason", employee.getFirstName());
        Str.equalsOrDie("Daniel", employee.getLastName());


        employee = store.load("456");
        Str.equalsOrDie("Paul", employee.getFirstName());
        Str.equalsOrDie("Tabor", employee.getLastName());

        employee = store.load("123");
        Str.equalsOrDie("Rick", employee.getFirstName());
        Str.equalsOrDie("Hightower", employee.getLastName());

    }

    @Test
    public void testBulkRemove() {


        Map<String, Employee> map = Maps.map(

                "123", new Employee("123", "Rick", "Hightower"),
                "456", new Employee("456", "Paul", "Tabor"),
                "789", new Employee("789", "Jason", "Daniel")

        );


        store.putAll(map);


        Employee employee;


        employee = store.load("789");
        Str.equalsOrDie("Jason", employee.getFirstName());
        Str.equalsOrDie("Daniel", employee.getLastName());


        employee = store.load("456");
        Str.equalsOrDie("Paul", employee.getFirstName());
        Str.equalsOrDie("Tabor", employee.getLastName());

        employee = store.load("123");
        Str.equalsOrDie("Rick", employee.getFirstName());
        Str.equalsOrDie("Hightower", employee.getLastName());


        store.removeAll(map.keySet());


        employee = store.load("123");

        ok = employee == null || die();

        employee = store.load("456");


        ok = employee == null || die();


    }

    @Test
    public void testSearch() {
        for (int index = 0; index < 100; index++) {

            store.put("zkey." + index, new Employee("zkey." + index, "Rick" + index, "Hightower"));
        }

        KeyValueIterable<String, Employee> entries = store.search("zkey.50");

        int count = 0;

        for (Entry<String, Employee> entry : entries) {
            puts(entry.key(), entry.value());
            count++;
        }




        ok = (count > 20 && count < 60) || die(count);



        for (Entry<String, Employee> entry : entries) {
            store.remove(entry.key());
        }

        entries.close();
    }

    @Test
    public void testIteration() {

        for (int index = 0; index < 100; index++) {

            store.put("iter." + index, new Employee("iter." + index, "Rick" + index, "Hightower"));
        }


        KeyValueIterable<String, Employee> entries = store.loadAll();

        int count = 0;

        for (Entry<String, Employee> entry : entries) {
            puts(entry.key(), entry.value());
            count++;
        }

        ok = (count >= 100) || die(count);



        for (Entry<String, Employee> entry : entries) {
            store.remove(entry.key());
        }
        entries.close();

    }


    public static class Employee implements Serializable {
        String firstName;
        String lastName;
        String id;

        public Employee(String id, String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.id = id;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Employee)) return false;

            Employee employee = (Employee) o;

            if (firstName != null ? !firstName.equals(employee.firstName) : employee.firstName != null) return false;
            if (id != null ? !id.equals(employee.id) : employee.id != null) return false;
            if (lastName != null ? !lastName.equals(employee.lastName) : employee.lastName != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = firstName != null ? firstName.hashCode() : 0;
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    ", id='" + id + '\'' +
                    '}';
        }
    }

}
