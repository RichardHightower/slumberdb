package info.slumberdb.vertx;

import info.slumberdb.rest.Request;
import info.slumberdb.rest.Response;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServerRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by Richard on 4/6/14.
 */
public class VertxUtils {


    /**
     * Takes a Vertx request and creates a   request object form it.
     *
     * @param request vertx request
     * @param body    body
     */
    public static Request request(HttpServerRequest request, String body) {

        return Request.request(request.method(), toMap(request.params()), body, request.path());

    }


    /**
     * Creates a request object in the case of an error during HTTP invoke.
     *
     * @param request vertx request
     * @param body    body
     * @return request
     */
    public static Request error(HttpServerRequest request, String body) {

        return Request.request(request.method(), toMap(request.params()), body, request.path());

    }


    /**
     * Converts a   response into vert.x response output.
     *
     * @param request  request
     * @param response response
     */
    public static void encodeResponse(HttpServerRequest request, Response response) {
        request.response().headers().add("Access-Control-Allow-Origin", "http://127.0.0.1:3030");
        request.response().setStatusCode(response.code()).end(response.response());
    }

    /**
     * Converts a multi map to a map.
     */
    public static Map<String, Object> toMap(MultiMap multiMap) {
        Map<String, Object> map = new HashMap<>(multiMap.size());
        for (String name : multiMap.names()) {
            List<String> all = multiMap.getAll(name);
            if (all.size() == 1) {
                map.put(name, all.get(0));
            } else if (all.size() == 0) {

            } else {
                map.put(name, all);
            }
        }
        return map;
    }
}
