package info.slumberdb.vertx;

import info.slumberdb.mailbox.MailBox;
import info.slumberdb.rest.BasicRestRequestHandler;
import info.slumberdb.rest.Response;
import info.slumberdb.rest.RestHandler;
import org.boon.Logger;
import org.boon.core.reflection.BeanUtils;
import org.boon.di.Context;
import org.boon.di.Inject;
import org.boon.json.serializers.impl.JsonSimpleSerializerImpl;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;

import java.io.File;
import java.util.List;
import java.util.Map;

import static info.slumberdb.rest.ResponseUtils.handleException;
import static info.slumberdb.vertx.VertxUtils.encodeResponse;
import static org.boon.Boon.*;
import static org.boon.di.DependencyInjection.context;
import static org.boon.di.DependencyInjection.objects;

/**
 * Vertx networking stack for HTTP/REST verticles
 * Created by Richard on 4/6/14.
 *
 * @author Rick Hightower
 */
public class RestVerticle extends Verticle {


    private final JsonSimpleSerializerImpl jsonSerializer = new JsonSimpleSerializerImpl();
    private Logger logger = configurableLogger(RestVerticle.class);
    @Inject
    private String namespace;

    @Inject
    private String configPath;


    private Context context;

    /**
     * Starts verticle.
     */
    public void start() {

        MailBox mailBox = new EventBusMailBox(vertx.eventBus());
        init(mailBox);


        initServiceMessaging();
    }


    /**
     * Used for testing so we can pass a mock mail box easily.
     * This also gets called by the verticle startup.
     *
     * @param mailBox
     */
    public void init(MailBox mailBox) {


        bootStrap(); //reads bootstrap (where do I find my config, who am I)
        readConfigContext(mailBox); //reads basic configuration for ports.

        /** Initializes dependency Injection container.*/
        context = context(
                objects(mailBox)
        );
        context.resolveProperties(this);


    }


    /**
     * Initializes the service message bus.
     * For now this lives in the same process, but it can be in another process in the near future.
     */
    private void initServiceMessaging() {
        logger.trace("initServiceMessaging", "entering");
        container.deployVerticle("info.slumberdb.vertx.ServiceHandlersVerticle",
                new Handler<AsyncResult<String>>() {
                    @Override
                    public void handle(AsyncResult<String> event) {
                        if (event.failed()) {
                            logger.fatal(event.cause(), "Unable to initialize service messaging");
                            event.cause().printStackTrace();
                        }
                    }
                }
        );
        logger.trace("initServiceMessaging", "leaving");

    }

    /**
     * Initializes a rest stack.
     */
    private void initializeREST(final RestHandler restHandler, int port) {

        HttpServer server = vertx.createHttpServer();

        server.requestHandler(new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {

                if (request.method().equals("POST")) {

                    handlePost(request, restHandler);

                } else {


                    handleRequest(request, null, restHandler);

                }

            }
        });
        server.listen(port);

        container.logger().info(sputs("Admin facing HTTP stack started", "working dir", new File(".").getAbsolutePath()));

        logger.trace("initializeAdminHTTP", "leaving");
    }


    /**
     * This handles both a GET and a POST.
     *
     * @param request          the http request from vert.x
     * @param requestBody      the request body if this is a post
     * @param httpStackHandler http stack handler
     */
    private void handleRequest(final HttpServerRequest request, String requestBody, RestHandler httpStackHandler) {

        org.boon.core.Handler requestHandler = new org.boon.core.Handler<Response>() {
            @Override
            public void handle(Response response) {
                encodeResponse(request, response);
            }
        };


        org.boon.core.Handler errorHandler = new org.boon.core.Handler<Response>() {
            @Override
            public void handle(Response response) {

                logger.error("handleRequest handleException", "URI",
                        request.uri(), "QUERY", request.query(), response.response());
                encodeResponse(request, response);
            }
        };


        try {

            httpStackHandler.handle(VertxUtils.request(request, requestBody), requestHandler);
        } catch (Exception ex) {
            handleException(VertxUtils.error(request, requestBody), ex, jsonSerializer, errorHandler);
        }


    }

    /**
     * Handles an HTTP Post
     *
     * @param request
     * @param httpStackHandler
     */
    private void handlePost(final HttpServerRequest request, final RestHandler httpStackHandler) {

        /* Create a buffer. */
        final Buffer body = new Buffer();


        /* Register a callback to load notified when we load the next block of gak from the POST. */
        request.dataHandler(new Handler<Buffer>() {
            public void handle(Buffer buffer) {
                body.appendBuffer(buffer);
            }
        });



        /* Register a callback to notify us that we got all of the body. */
        request.endHandler(new VoidHandler() {
            public void handle() {

                handleRequest(request, body.toString(), httpStackHandler);

            }
        });
    }


    /**
     * Stops Verticle.
     */
    public void stop() {
        super.stop();
    }


    /**
     * Bootstrap config.
     * <p/>
     * This reads a small injectable config context from a JSON file called bootstrap.json.
     * <p/>
     * <p/>
     * We first search for:
     * <p/>
     * <p/>
     * <p/>
     * /opt/ / /bootstrap.json
     * <p/>
     * <p/>
     * If that is not found then we search for:
     * <p/>
     * classpath:// / / /bootstrap.json
     */
    private void bootStrap() {

        try {


            Context bootStrap = readConfig("bootstrap", "/etc/slumberdb/conf");

            bootStrap.resolvePropertiesIgnoreRequired(this);

            logger.info("REST STACK USING NAMESPACE", this.namespace);

        } catch (Exception ex) {
            logger.fatal(ex, "unable to perform bootstrap");
        }
    }


    /**
     * Reads the configuration
     *
     * @see RestVerticle#bootStrap()
     */
    private void readConfigContext(MailBox mailBox) {

        logger.info(this.namespace, this.configPath);
        context = readConfig(this.namespace, this.configPath);
        context.resolvePropertiesIgnoreRequired(this);

        List<Map<String, Object>> endpoints = context.get(List.class, "endpoints");

        logger.info("endpoints", endpoints);

        for (Map<String, Object> endpoint : endpoints) {

            logger.info("endpoint", endpoint);


            Number number = (Number) endpoint.get("port");
            int port = number.intValue();


            final BasicRestRequestHandler restHandler = new BasicRestRequestHandler();

            BeanUtils.copyProperties(restHandler, endpoint);

            BeanUtils.injectIntoProperty(restHandler, "mailBox", mailBox);

            context.resolveProperties(restHandler);


            if (vertx != null) {
                initializeREST(restHandler, port);
            }
        }


    }


}
