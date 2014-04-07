package info.slumberdb;

import com.hazelcast.core.*;
import com.hazelcast.config.*;

import java.io.Serializable;
import java.util.Map;

public class Main {



    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:3306/slumberdb";
        String userName = "slumber";
        String password = "slumber1234";
        String table = "json-employee-hazel";


        MapConfig mapCfg = new MapConfig();
        mapCfg.setName("employees");
        MapStoreConfig mapStoreCfg = new MapStoreConfig();
        mapStoreCfg.setImplementation(new HazelCastMySQLJSONMapStore(url, userName,
                password, table, Employee.class));
        mapStoreCfg.setEnabled(true);
        mapCfg.setMapStoreConfig(mapStoreCfg);


        Config cfg = new Config();
        cfg.addMapConfig(mapCfg);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);




        Map<String, Employee> mapCustomers = instance.getMap("employees");
        mapCustomers.put("1", new Employee("Bob", "Jones"));
    }

    public static class Employee implements Serializable {
        String firstName;
        String lastName;
        String id;

        public Employee(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
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
