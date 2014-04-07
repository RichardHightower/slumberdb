package info.slumberdb.rest;

import java.util.Map;


/**
 * Represents a REST request object.
 */
public class Request {

    final String method;
    final Map<String, Object> params;
    final String body;
    final String path;





    public static Request request( String method, Map<String, Object> params, String body, String path ) {
        return new Request(method, params, body, path);
    }



    private Request( String method, Map<String, Object> params, String body, String path ) {
        this.method = method;
        this.params = params;
        this.body = body;
        this.path = path;
    }

    public String method() {
        return method;
    }

    public Map<String, Object> params() {
        return params;
    }

    public String body() {
        return body;
    }

    public String path() {
        return path;
    }






}
