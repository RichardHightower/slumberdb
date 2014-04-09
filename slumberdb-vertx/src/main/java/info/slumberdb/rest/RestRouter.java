package info.slumberdb.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.boon.Exceptions.die;

/**
 * Routes requests handles URI dispatch.
 */
public class RestRouter {


    private String uriBase;
    private Set<String> handlerPaths;
    private Map<String, String> cachedDispatchIds = new HashMap<>();


    public RestRouter(String uriBase, Set<String> handlers) {
        this.uriBase = uriBase;
        this.handlerPaths = handlers;
    }

    public String dispatchId(String uri) {
        String dispatchId = cachedDispatchIds.get(uri);


        /**
         * Find the dispatch id if possible.
         */
        for (String id : handlerPaths) {
            if ( uri.endsWith(id) ) {
                dispatchId = id;
                break;
            }
        }


        /** If we found, load happy and store the results in
         * the cache so we don't have to do this again.
         */
        if (dispatchId!=null) {
            cachedDispatchIds.put( uri, dispatchId );
        } else {
            die("We were unable to find a handler for URI", uri,
                    "This is for baseURI", this.uriBase,
                    "The handlers we know about are", handlerPaths);
        }
        return dispatchId;
    }

    int routes() {
        return this.cachedDispatchIds.size();
    }

}
