package info.slumberdb;


import java.io.Serializable;

/**
 * Represents a CRUD operation with a key / value store.
 * @param <KEY> Key (The key can be a list)
 * @param <PAYLOAD> payload (The payload can be a map)
 */
public class  CrudOperation <KEY, PAYLOAD> implements Serializable {
    private final KEY key;
    private final PAYLOAD payload;


    public CrudOperation(KEY key, PAYLOAD payload) {
        this.key = key;
        this.payload = payload;
    }

    public CrudOperation() {
        key = null;
        payload = null;
    }
}
