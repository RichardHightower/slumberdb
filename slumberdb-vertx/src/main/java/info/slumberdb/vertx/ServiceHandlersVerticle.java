package info.slumberdb.vertx;

import info.slumberdb.mailbox.MailBox;
import org.boon.Logger;
import org.boon.di.Context;
import org.boon.di.Inject;
import org.vertx.java.platform.Verticle;

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
    private String serviceNameSpace;
    @Inject
    private String serviceConfigPath;


    /**
     * Starts verticle.
     */
    public void start() {

        MailBox mailBox = new EventBusMailBox(vertx.eventBus());

        init(mailBox);

//        long timerID = vertx.setPeriodic(1000 * 60, new Handler<Long>() {
//            public void handle(Long timerID) {
//            }
//        });

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

            Context context = bootstrap();


//            context.add(objects(mailBox,
//                    //add objects here
//            );

//            context.resolveProperties(this);


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
    private Context bootstrap() {


        logger.info("Starting Service Message STACK bootstrap");

        Context bootStrap = readConfig("bootstrap", "/opt/slumberdb/conf");
        bootStrap.resolvePropertiesIgnoreRequired(this);
        Context context = readConfig(this.serviceNameSpace, this.serviceConfigPath);
        return context;
    }



    /**
     * Vert.x calls the stop method when the verticle is un-deployed.
     * Put any cleanup code for your verticle in here
     */
    public void stop() {

    }

}
