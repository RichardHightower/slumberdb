package info.slumberdb;

import info.slumberdb.rest.*;
import org.boon.Str;
import org.boon.core.AsyncFunction;
import org.boon.core.Handler;
import org.boon.core.reflection.ClassMeta;
import org.boon.core.reflection.MethodAccess;
import org.boon.core.reflection.Reflection;
import org.boon.di.PostConstruct;

import java.util.HashMap;
import java.util.Map;

import static org.boon.Boon.puts;

public class Client extends BaseRequestHandler implements StringKeyValueStore {


    @PostConstruct
    public void init() {
        this.postHandlers = super.initDefaultHandlers("client");
        this.getHandlers = super.metaHandlerMap();

        puts (postHandlers);
        super.init("/slumberdb/");
    }



    public void put(Entry<String, String> entry) {
        this.put(entry.key(), entry.value());
    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public void putAll(Map<String, String> values) {

    }

    @Override
    public void removeAll(Iterable<String> keys) {

    }

    @Override
    public void remove(String key) {

    }

    @Override
    public KeyValueIterable<String, String> search(String startKey) {
        return null;
    }

    @Override
    public KeyValueIterable<String, String> loadAll() {
        return null;
    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void close() {

    }
}
