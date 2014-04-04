Slumber DB - Key Value Store for JSON / REST
=========

The JSON database for REST and Websocket storage.


This is an effort to write key / value stores for JSR-107 RI
and one for HazelCast MapStore.

The goals are simple. Write a MySQL, LevelDB, LMDB, and RocksDB key / value store
for Java serialization and JSON for HazelCast MapStore and JSR-107 RI Cache Stores.

The MySQL version, for example, will use Boon JSON serialization and kryo.


Boon is the fastest JSON serialization.
Kryo is the fastest Java serialization.

The plan is to use **leveldbjni** for the leveldb implementation, and later Vertx for server and replication support.

Goals
=========

Provide a simple key/value store.

Allow store by key.
Read by key.
Store many key/value pairs.
Read many key/value pairs.
Delete a key.
Delete a batch of keys.
Search by key.
Batch update operations for JSON and Java objects (not implemented yet).

We have an implementation in LevelDB for Strings, JSON and binary.



Interface
==========

The main interface(s) for SlumberDB are as follows:

```java

public interface KeyValueStore <K, V> extends Closeable, Flushable{

    void put(K key, V value);


    void putAll(Map<K, V> values);

    void removeAll(Iterable<K> keys);

    void remove(K key);

    KeyValueIterable<K, V> search(K startKey);

    KeyValueIterable<K, V> loadAll();

    V get(K key);


    void close();

    void flush();

}

```

The focus is on being a store.
There is some rudimentary read operations for faulting in-memory cache operations.



License
=========

SlumberDB is Apache 2.0 license.

Related projects
=========

**LevelDB** is a lightweight database by **Google** modeled after BigTable tablet store.
LevelDB gets used by Chrome.

**RocksDB** is a server-side version of LevelDB by **Facebook** that reportedly is more scalable than LevelDB.

**MySQL** is well MySQL. We are using it as a table with two columns. One column is indexed.

**LMDB** is an fast, compact key/value store which gets used by the OpenLDAP Project.


**Kryo** is the fastest and efficient object graph serialization for the JVM.



See Kryo at: https://github.com/EsotericSoftware/kryo

See Boon at: https://github.com/RichardHightower/boon

See LevelDBJNI at: https://github.com/fusesource/leveldbjni

See LevelDB at: https://code.google.com/p/leveldb/

See RocksDB at: http://rocksdb.org/

See RocksDBJNI at: https://github.com/fusesource/rocksdbjni

See LMDB: http://symas.com/mdb/

See LMDBJNI: https://github.com/chirino/lmdbjni


Thanks
=========

Special thanks to Hiram Chirino for writing **leveldbjni**, **lmdbjni** and **rocksdbjni**.
Without Hiram, Apache Apollo hero, none of this would be possible.

See Mr. Chirino at:
https://github.com/chirino


Special thanks to Tim Fox for writing Vertx. Tim is the author of Vertx which leads the charts for fast JVM based web servers.
Vertx is so much more than a web server.

https://github.com/purplefox

We plan on using Vertx for replication and client/server networking support.

http://www.infoq.com/news/2011/12/apollo-benchmarks


Primary Author
=========

Rick Hightower works on JSR-107 and JSR-347 as well as Boon
which has an in-memory query engine, and a fast JSON parser/serializer.

