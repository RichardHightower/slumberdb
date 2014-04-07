package info.slumberdb;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.boon.Exceptions;
import org.boon.Logger;

import java.sql.*;
import java.util.*;

import static org.boon.Boon.configurableLogger;
import static org.boon.Boon.puts;
import static org.boon.Boon.sputs;

/**
 * Created by Richard on 4/4/14.
 */
public class SimpleStringBinaryKeyValueStoreMySQL implements KeyValueStore<String, byte[]>{

    private String url;
    private String userName;
    private String password;
    private String table;
    private Connection connection;
    private String insertStatementSQL;
    private String selectStatementSQL;
    private String searchStatementSQL;
    private String createStatementSQL;
    private  String deleteStatementSQL;

    private String selectKeysSQL;

    private  String tableExistsSQL;

    private PreparedStatement insert;
    private PreparedStatement delete;

    private PreparedStatement select;
    private PreparedStatement search;
    private PreparedStatement loadAll;


    private Logger logger = configurableLogger(SimpleStringKeyValueStoreMySQL.class);
    private String loadAllSQL;
    private boolean useBatch = true;

    private int batchSize = 100;
    private PreparedStatement allKeys;

    public SimpleStringBinaryKeyValueStoreMySQL(String url, String userName, String password, String table) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.table = table;


        createSQL(table);

        this.connection = connection();


        createTableIfNeeded(table);

        createPreparedStatements();


    }

    private void createSQL(String table) {
        this.insertStatementSQL = "replace into `" + table + "` (kv_key, kv_value) values (?,?);";
        this.selectStatementSQL = "select kv_value from `" + table + "` where kv_key = ?;";
        this.searchStatementSQL = "select kv_key, kv_value from `" + table + "` where kv_key >= ?;";
        this.loadAllSQL = "select kv_key, kv_value from `" + table +"`;";
        this.selectKeysSQL = "select kv_key from `" + table +"`;";


        this.deleteStatementSQL = "delete  from `" + table + "` where kv_key = ?;";

        this.tableExistsSQL = "select * from `" + table + "` where 1!=1;";

        this.createStatementSQL = "\n" +
                "CREATE TABLE " +  "`" + table + "` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                "  `kv_key` varchar(80) DEFAULT NULL,\n" +
                "  `kv_value` BLOB,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY  `" + table + "_kv_key_idx` (`kv_key`)\n" +
                ");\n";
    }

    private void createPreparedStatements() {
        try {

            insert = connection.prepareStatement(insertStatementSQL);

            delete = connection.prepareStatement(deleteStatementSQL);

            select = connection.prepareStatement(selectStatementSQL);

            search = connection.prepareStatement(searchStatementSQL);

            loadAll = connection.prepareStatement(loadAllSQL);

            allKeys = connection.prepareStatement(selectKeysSQL);

        } catch (SQLException e) {
            handle("Unable to create prepared statements", e);
        }
    }

    private void createTableIfNeeded(String table) {
        try {

            puts(tableExistsSQL);

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(tableExistsSQL);
            resultSet.next();


        } catch (SQLException e) {

            try {

                Statement statement = connection.createStatement();
                statement.execute(createStatementSQL);

            } catch (SQLException e1) {
                logger.error(e1, "Unable to create table", table, "\n", createStatementSQL);
                handle("Unable to create prepare table", e);

            }
        }
    }

    @Override
    public void put(String key, byte[] value) {

        try {
            insert.setString(1, key);
            insert.setBytes(2, value);
            insert.executeUpdate();

        } catch (SQLException e) {
            handle(sputs("Unable to insert key", key, "value", value), e);
        }


    }


    public void putAllUseBatch(Map<String, byte[]> values) {

        int count = 0;
        try {

            Set<Map.Entry<String, byte[]>> entries = values.entrySet();

            for (Map.Entry<String, byte[]> entry : entries) {
                String key = entry.getKey();
                byte[] value = entry.getValue();
                insert.setString(1, key);
                insert.setBytes(2, value);
                insert.addBatch();

                if (count == batchSize) {
                    count = 0;
                    insert.executeBatch();
                } else {
                    count++;
                }
            }

            insert.executeBatch();

        } catch (SQLException e) {
            handle("Unable to putALl values", e);
            connection = connection();

        }
    }

    public void putAllUseTransaction(Map<String, byte[]> values) {

        try {
            connection.setAutoCommit(false);

            Set<Map.Entry<String, byte[]>> entries = values.entrySet();

            for (Map.Entry<String, byte[]> entry : entries) {
                String key = entry.getKey();
                byte[] value = entry.getValue();
                insert.setString(1, key);
                insert.setBytes(2, value);
                insert.executeUpdate();
            }

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            }  catch (SQLException e1) {
                logger.warn("Unable to rollback exception", e1);
            }
            handle("Unable to putALl values", e);

        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                logger.warn("Unable to set auto commit back to true", e);
                connection = connection();
            }

        }

    }

    @Override
    public void putAll(Map<String, byte[]> values) {


        if (useBatch) {
            putAllUseBatch(values);
        } else {
            putAllUseTransaction(values);
        }
    }

    @Override
    public void removeAll(Iterable<String> keys) {
        if (useBatch) {
            removeAllUseBatch(keys);
        }else {
            removeAllUseTransaction(keys);
        }
    }

    public void removeAllUseBatch(Iterable<String> keys) {

        try {

            for (String key : keys) {
                delete.setString(1, key);
                delete.addBatch();
            }

            delete.executeBatch();

        } catch (SQLException e) {
            handle("Unable to removeAll values", e);
        }

    }


    public void removeAllUseTransaction(Iterable<String> keys) {

        try {
            connection.setAutoCommit(false);


            for (String key : keys) {
                this.remove(key);
            }

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            }  catch (SQLException e1) {
                logger.warn("Unable to rollback exception", e1);
            }
            handle("Unable to putALl values", e);

        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                logger.warn("Unable to set auto commit back to true", e);
                connection = connection();
            }

        }

    }

    @Override
    public void remove(String key) {

        try {
            delete.setString(1, key);
            delete.executeUpdate();
        } catch (SQLException e) {
            handle(sputs("Unable to remove key", key), e);
        }

    }

    @Override
    public KeyValueIterable<String, byte[]> search( final String startKey) {


        try {
            search.setString(1, startKey+"%");
            final ResultSet resultSet = search.executeQuery();

            return new KeyValueIterable<String, byte[]> () {

                @Override
                public void close() {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        handle("Unable to close result set for search query for " + startKey, e);
                    }
                }

                @Override
                public Iterator<Entry<String, byte[]>> iterator() {

                    return new Iterator<Entry<String, byte[]>>() {
                        @Override
                        public boolean hasNext() {
                            try {
                                return resultSet.next();
                            } catch (SQLException e) {
                                handle("Unable to call next() for result set for search query for " + startKey, e);
                                return false;
                            }
                        }

                        @Override
                        public Entry<String, byte[]> next() {
                            try {

                                String key = resultSet.getString(1);
                                byte[] value = resultSet.getBytes(2);
                                return new Entry<>(key, value);
                            } catch (SQLException e) {
                                handle("Unable to extract values for search query for " + startKey, e);
                                return null;
                            }

                        }

                        @Override
                        public void remove() {

                        }
                    };
                }
            };



        } catch (SQLException e) {
            handle(sputs("Unable to search records search key", startKey, "\nquery=", this.searchStatementSQL) , e);
            return null;
        }
    }

    @Override
    public KeyValueIterable<String, byte[]> loadAll() {

        try {
            final ResultSet resultSet = loadAll.executeQuery();

            return new KeyValueIterable<String, byte[]> () {

                @Override
                public void close() {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        handle("Unable to close result set for loadAll query", e);
                    }
                }

                @Override
                public Iterator<Entry<String, byte[]>> iterator() {

                    return new Iterator<Entry<String, byte[]>>() {
                        @Override
                        public boolean hasNext() {
                            try {
                                return resultSet.next();
                            } catch (SQLException e) {
                                handle("Unable to call next() for result set for loadAll query", e);
                                return false;
                            }
                        }

                        @Override
                        public Entry<String, byte[]> next() {
                            try {

                                String key = resultSet.getString(1);
                                byte[] value = resultSet.getBytes(2);
                                return new Entry<>(key, value);
                            } catch (SQLException e) {
                                handle("Unable to extract values for loadAll query", e);
                                return null;
                            }

                        }

                        @Override
                        public void remove() {

                        }
                    };
                }
            };



        } catch (SQLException e) {
            handle("Unable to load all records", e);
            return null;
        }
    }

    @Override
    public Collection<String> loadAllKeys() {

        LinkedHashSet<String> set = new LinkedHashSet<>();
        ResultSet resultSet = null;

        try {

            resultSet = allKeys.executeQuery();

            while (resultSet.next()) {
                String key = resultSet.getString(1);
                set.add(key);
            }
        }
        catch (SQLException e) {
                handle("Unable to call next() for result set for loadAllKeys query", e);
        }finally {
            closeResultSet(resultSet);
        }

        return set;
    }

    private void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                Exceptions.handle(e);
            }
        }
    }

    @Override
    public byte[] get(String key) {

        byte[] value;
        try {
            select.setString(1, key);
            final ResultSet resultSet = select.executeQuery();


            if (resultSet.next()) {
                value = resultSet.getBytes(1);
            } else {
                value = null;
            }

        } catch (SQLException ex) {
            handle("Unable to load " + key, ex);
            return null;
        }
        return value;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.warn("Problem closing", e);
        }
    }


    private Connection connection()  {
        try {
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setURL(url);
            dataSource.setPassword(password);
            dataSource.setUser(userName);
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            return connection;
        } catch (SQLException sqlException) {
            handle("Unable to connect", sqlException);
            return null;
        }
    }

    private void handle(String message, SQLException sqlException) {


        logger.error(message, sqlException);

        while ((sqlException = sqlException.getNextException())!=null) {
            logger.error(message, sqlException);
        }

        Exceptions.handle(message, sqlException);
        connection = connection();

    }

}

