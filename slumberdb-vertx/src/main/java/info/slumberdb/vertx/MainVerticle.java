package info.slumberdb.vertx;

import org.boon.Logger;
import org.boon.di.Inject;
import org.boon.json.serializers.impl.JsonSimpleSerializerImpl;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.platform.Verticle;

import static org.boon.Boon.configurableLogger;

/**
 * Created by Richard on 4/13/14.
 */
public class MainVerticle extends Verticle {

    private final JsonSimpleSerializerImpl jsonSerializer = new JsonSimpleSerializerImpl();
    private Logger logger = configurableLogger(RestVerticle.class);
    @Inject
    private String namespace;

    @Inject
    private String configPath;


    public void start() {

        container.deployVerticle("info.slumberdb.vertx.RestVerticle", 10);

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
}
