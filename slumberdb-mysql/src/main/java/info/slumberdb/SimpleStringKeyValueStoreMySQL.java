package info.slumberdb;

import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.boon.Exceptions;
import org.boon.Logger;

import static org.boon.Boon.configurableLogger;
import static org.boon.Boon.puts;
import static org.boon.Boon.sputs;

public class SimpleStringKeyValueStoreMySQL implements StringKeyValueStore{

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

    private  String tableExistsSQL;

    private PreparedStatement insert;
    private PreparedStatement delete;

    private PreparedStatement select;
    private PreparedStatement search;
    private PreparedStatement loadAll;


    private Logger logger = configurableLogger(SimpleStringKeyValueStoreMySQL.class);
    private String loadAllSQL;

    public SimpleStringKeyValueStoreMySQL(String url, String userName, String password, String table) {
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

        this.deleteStatementSQL = "delete  from `" + table + "` where kv_key = ?;";

        this.tableExistsSQL = "select * from `" + table + "` where 1!=1;";

        this.createStatementSQL = "\n" +
                "CREATE TABLE " +  "`" + table + "` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                "  `kv_key` varchar(80) DEFAULT NULL,\n" +
                "  `kv_value` TEXT,\n" +
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
    public void put(String key, String value) {

        try {
            insert.setString(1, key);
            insert.setString(2, value);
            insert.executeUpdate();

        } catch (SQLException e) {
            handle(sputs("Unable to insert key", key, "value", value), e);
        }


    }

    @Override
    public void putAll(Map<String, String> values) {

        try {
            connection.setAutoCommit(false);

            Set<Map.Entry<String, String>> entries = values.entrySet();

            for (Map.Entry<String, String> entry : entries) {
                this.put(entry.getKey(), entry.getValue());
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
    public void removeAll(Iterable<String> keys) {

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
    public void updateAll(Iterable<CrudOperation> updates) {

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
    public KeyValueIterable<String, String> search( final String startKey) {


        try {
            search.setString(1, startKey+"%");
            final ResultSet resultSet = search.executeQuery();

            return new KeyValueIterable<String, String> () {

                @Override
                public void close() {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        handle("Unable to close result set for search query for " + startKey, e);
                    }
                }

                @Override
                public Iterator<Entry<String, String>> iterator() {

                    return new Iterator<Entry<String, String>>() {
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
                        public Entry<String, String> next() {
                            try {

                                String key = resultSet.getString(1);
                                String value = resultSet.getString(2);
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
    public KeyValueIterable<String, String> loadAll() {

        try {
            final ResultSet resultSet = loadAll.executeQuery();

            return new KeyValueIterable<String, String> () {

                @Override
                public void close() {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        handle("Unable to close result set for loadAll query", e);
                    }
                }

                @Override
                public Iterator<Entry<String, String>> iterator() {

                    return new Iterator<Entry<String, String>>() {
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
                        public Entry<String, String> next() {
                            try {

                                String key = resultSet.getString(1);
                                String value = resultSet.getString(2);
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
    public String get(String key) {

        String value;
        try {
            select.setString(1, key);
            final ResultSet resultSet = select.executeQuery();


            if (resultSet.next()) {
                value = resultSet.getString(1);
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

    @Override
    public void flush() {
        try {
            connection.commit();
        } catch (SQLException e) {
            handle("Unable to commit", e);
        }

    }



    private Connection connection()  {
        try {
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setURL(url);
            dataSource.setPassword(password);
            dataSource.setUser(userName);
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
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
    }

}
