package info.slumberdb.vertx;

import info.slumberdb.mailbox.MailBox;
import info.slumberdb.mailbox.MailboxDispatch;
import info.slumberdb.rest.BasicRestRequestHandler;
import org.boon.Logger;
import org.boon.core.reflection.BeanUtils;
import org.boon.core.reflection.Reflection;
import org.boon.di.Context;
import org.boon.di.Inject;
import org.vertx.java.platform.Verticle;

import java.util.List;
import java.util.Map;

import static org.boon.Boon.configurableLogger;
import static org.boon.Boon.readConfig;


/**
 * <p>
 *
 * This receives messages posted to the queue async from the HTTP REST calls.
 * This can live on another box or process and will likely live there someday soon.
 *
 * This can live in a different module or in a different JVM.
 * </p>
 */
public class ServiceHandlersVerticle extends Verticle {

    public final static Class<ServiceHandlersVerticle> serviceMessagingVerticle = ServiceHandlersVerticle.class;

    private Logger logger = configurableLogger( serviceMessagingVerticle );
    @Inject
    private String namespace;
    @Inject
    private String configPath;


    /**
     * Starts verticle.
     */
    public void start() {

        MailBox mailBox = new EventBusMailBox(vertx.eventBus());

        init(mailBox);


    }


    /**
     * This exists to make testing a bit easier.
     * pass in a mailbox which allows us to test this verticle with a unit test by mocking a mailbox.
     *
     * @param mailBox mail box which is an abstraction over vert.x messaging.
     */
    public void init(MailBox mailBox) {

        try {

            logger.info("Initializing ", serviceMessagingVerticle.getName(), "mailbox");

            Context context = bootstrap(mailBox);


        }
        catch(Exception ex) {

            ex.printStackTrace(System.err);
            logger.fatal(ex, "Unable to load service verticle");
        }

    }

    /**
     * Bootstrap config.
     * @return
     */
    private Context bootstrap(MailBox mailBox) {


        logger.info("Starting Service Message STACK bootstrap");

        Context bootStrap = readConfig("bootstrap", "/etc/slumberdb/conf");
        bootStrap.resolvePropertiesIgnoreRequired(this);
        Context context = readConfig(this.namespace, this.configPath);



        List<Map<String, Object>> endpoints = context.get(List.class, "endpoints");

        logger.info("endpoints", endpoints);

        for (Map<String, Object> endpoint : endpoints) {

            logger.info("endpoint", endpoint);




            final MailboxDispatch mailboxDispatch = new MailboxDispatch(mailBox);


            String type = (String)endpoint.get("type");

            Object object = Reflection.newInstance(type);

            BeanUtils.injectIntoProperty(mailboxDispatch, "service", object);


            context.resolvePropertiesIgnoreRequired(mailboxDispatch);


        }

        return context;
    }



    /**
     * Vert.x calls the stop method when the verticle is un-deployed.
     * Put any cleanup code for your verticle in here
     */
    public void stop() {

    }

}
