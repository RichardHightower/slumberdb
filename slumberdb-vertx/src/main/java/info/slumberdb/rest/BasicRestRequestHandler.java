package info.slumberdb.rest;

import info.slumberdb.mailbox.MailBox;
import info.slumberdb.mailbox.Message;
import org.boon.Maps;
import org.boon.Str;
import org.boon.core.AsyncFunction;
import org.boon.core.Handler;
import org.boon.core.reflection.ClassMeta;
import org.boon.core.reflection.MethodAccess;
import org.boon.di.Inject;
import org.boon.di.PostConstruct;
import org.boon.json.JsonParserAndMapper;
import org.boon.json.JsonParserFactory;
import org.boon.json.serializers.impl.JsonSimpleSerializerImpl;


import java.util.HashMap;
import java.util.Map;

import static info.slumberdb.rest.ResponseUtils.extractPublicAPI;
import static org.boon.Boon.puts;
import static org.boon.Boon.sputs;
import static org.boon.Maps.map;

/**
 * Created by Richard on 4/6/14.
 * Base class for handling REST like requests.
 * This handles serializing as well.
 */
public class BasicRestRequestHandler implements RestHandler {



    @Inject
    protected MailBox mailBox;

    private String baseUri;

    private Class<?> type;

    private String serviceName;

    int port;


    @PostConstruct
    public void init() {
        this.postHandlers = initDefaultHandlers(type, serviceName);
        this.getHandlers = metaHandlerMap();

        puts (postHandlers);
        init(baseUri);
    }


    protected void sendMessage(final String address, final Request request, final Handler<String> handler) {


        mailBox.requestReply(address, request.body(),
                new Handler<Message>() {
                    @Override
                    public void handle(Message event) {
                        String results = event.body();
                        handler.handle(results);
                    }
                });

    }


    /**
     * Serialize JSON calls to service methods
     */
    private final JsonSimpleSerializerImpl jsonSerializer = new JsonSimpleSerializerImpl();


    /**
     * Parse JSON posts
     */
    private final JsonParserAndMapper jsonParser = new JsonParserFactory().create ();

    /**
     * Call/Message Router for REST GET Messages
     */
    private RestRouter requestRouter;


    /**
     * Call/Message Router for REST POST Messages
     */
    private RestRouter postRouter;






    /**
     * Holds the http get request handlers. Maps GET URIs to service calls.
     */
    protected Map<String, RequestBinding> getHandlers = null;


    /**
     * Holds the http get request handlers. Maps POST URIs to service calls.
     */
    protected Map<String, RequestBinding> postHandlers = null;


    /**
     * Lookup the binding
     * @param request request
     * @param responseHandler response handler
     * @param method method
     * @param uri uri
     * @return requestBinding
     */
    private RequestBinding lookupBinding(Request request, Handler<Response> responseHandler, String method, String uri) {
        RequestBinding binding = null;


        if ( method.equals( "GET" ) ) {

            String dispatchId = requestRouter.dispatchId(uri);
            binding = getHandlers.get( dispatchId );

        } else if ( method.equals( "POST" ) ) {


            String dispatchId = postRouter.dispatchId(uri);
            binding = postHandlers.get( dispatchId );

        } else {
            handleNoHandler( request, responseHandler );
        }


        return binding;
    }


    /**
     * Used if we were unable to handle the incoming URI.
     * @param request request
     * @param responseHandler response handler
     */
    protected void handleNoHandler(final Request request, Handler<Response> responseHandler) {
        String json = toJson(
                Maps.map(
                        "message", sputs("Unable to find handler for", request.method()),
                        "path", request.path(),
                        "params", request.params()
                )
        );

        responseHandler.handle(Response.response(json, 501));
    }


    /**
     * Converts an object to JSON
     * @param object object we want to convert
     * @return String json version of the object
     */
    protected String toJson( Object object ) {
        return jsonSerializer.serialize(object).toString();
    }

    /**
     * Convert from JSON to a specific type.
     * @param json json
     * @param cls cls
     * @param <T> type
     * @return object converted from JSON
     */
    protected <T> T fromJson( String json, Class<T> cls ) {
        return jsonParser.parse(cls, json);
    }

    /**
     * Initialize routers.
     * @param uri
     */
    public void init(String uri) {
        this.baseUri = uri;

        if (!baseUri.endsWith("/")) {
            baseUri = Str.add(baseUri,"/");
        }
        this.requestRouter = new RestRouter( uri, this.getHandlers.keySet() );
        this.postRouter = new RestRouter( uri, this.postHandlers.keySet() );
    }







    /**
     * Holds all of the GET RequestMappings for meta
     */
    protected Map<String, RequestBinding> metaHandlerMap() {




        RequestBinding showRequestMappings = RequestBinding.binding(Object.class, Object.class,
                new AsyncFunction<Request, String>() {

                    public void apply(Request request, Handler<String> handler) {
                        handler.handle( extractPublicAPI(getHandlers) );

                    }
                }
        );


        RequestBinding showPostMappings = RequestBinding.binding(Object.class, Object.class,
                new AsyncFunction<Request, String>() {

                    public void apply(Request request, Handler<String> handler) {
                        handler.handle( extractPublicAPI(postHandlers) );
                    }
                }
        );


        return map(
                "rest/get/", showRequestMappings,
                "rest/post/", showPostMappings

        );
    }




    /** Handles an HTTP request.
     *
     * Uses the dispatch ID to lookup the request binding, and then invokes the request binding.
     *
     *
     * */
    public void handle( final Request request, final Handler<Response> responseHandler ) {

        try {

            final String method = request.method();



            RequestBinding binding = lookupBinding(request, responseHandler, method, request.path());


            if ( binding == null ) {
                handleNoHandler( request, responseHandler);
                return;
            }

            binding.function.apply( request, new Handler<String>() {
                @Override
                public void handle(String event) {
                    responseHandler.handle(Response.response(event));
                }
            });

        } catch ( Exception ex ) {
            ResponseUtils.handleException(request, ex, jsonSerializer, responseHandler);
        }

    }



    public
    Map<String, RequestBinding>  initDefaultHandlers(Class<?> type, final String serviceName) {

        ClassMeta meta = ClassMeta.classMeta(type);

        Map<String, RequestBinding> bindingMap = new HashMap<>();

        final Iterable<MethodAccess> methods = meta.methods();

        for (final MethodAccess access : methods) {

            final Class<?>[] parameterTypes = access.parameterTypes();
            if (parameterTypes.length!=1) {
                continue;
            }

            Class<?> bodyType = parameterTypes[0];


            RequestBinding requestBinding = RequestBinding.binding(bodyType,
                    access.returnType(), new AsyncFunction<Request, String>() {
                        @Override
                        public void apply(Request request, Handler<String> handler) {
                            sendMessage(Str.add(serviceName, ".", access.name()), request, handler);
                        }
                    });
            bindingMap.put(Str.add(baseUri, serviceName, "/", access.name()), requestBinding);
        }

        return bindingMap;

    }




}

