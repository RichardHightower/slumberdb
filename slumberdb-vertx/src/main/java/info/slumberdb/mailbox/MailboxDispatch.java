package info.slumberdb.mailbox;

import info.slumberdb.rest.ResponseUtils;
import org.boon.Boon;
import org.boon.Exceptions;
import org.boon.Logger;
import org.boon.Str;
import org.boon.core.Conversions;
import org.boon.core.Handler;
import org.boon.core.reflection.ClassMeta;
import org.boon.core.reflection.MethodAccess;
import org.boon.di.Inject;
import org.boon.di.PostConstruct;
import org.boon.json.JsonParserAndMapper;
import org.boon.json.JsonParserFactory;
import org.boon.json.serializers.impl.JsonSimpleSerializerImpl;
import org.boon.primitive.CharBuf;

import static org.boon.Boon.configurableLogger;
import static org.boon.Exceptions.die;
import static org.boon.Exceptions.dieIfAnyParametersAreNull;
import static org.boon.Exceptions.requireNonNulls;
import static org.boon.Maps.map;
import static org.boon.Ok.okOrDie;
import static org.boon.Str.camelCaseLower;
import static org.boon.Str.join;
import static org.boon.Str.underBarCase;
import static org.boon.core.reflection.ClassMeta.classMeta;
import static org.boon.core.reflection.Invoker.invokeMethodFromObjectArg;
import static org.boon.core.reflection.Reflection.respondsTo;
import static org.boon.primitive.Chr.multiply;


/**
 * This reads messages from mail box and marshals calls to services.
 * @author Rick Hightower
 */
public class MailboxDispatch {


    private Logger logger = configurableLogger(MailboxDispatch.class);
    private final JsonSimpleSerializerImpl jsonSerializer = new JsonSimpleSerializerImpl();
    private final JsonParserAndMapper jsonParser = new JsonParserFactory().create ();

    private Object service;

    @PostConstruct
    private void init() {
        registerServiceCalls(service.getClass(), service);
    }

    /**
     * Used for performance critical debugging to avoid debug lookup for the logger.
     * It is final so that the JIT can optimize if blocks that use it out at runtime.
     */
    private final boolean debug;

    private MailBox mailBox;


    public MailboxDispatch(MailBox mailBox) {

        this.mailBox = mailBox;
        debug = Boon.debugOn();

    }

    /** Allows subclasses to register service calls. */
    public void registerServiceCalls(final String serviceName, final Object service, final String... methods) {
        doRegisterServiceCall(false, serviceName, service, methods);

    }

    /** Allows subclasses to register service calls.
     *
     * @param serviceInterface serviceInterface to make calls to
     * @param service service we are calling.
     */
    public void registerServiceCalls(final Class<?> serviceInterface, final Object service ) {
        requireNonNulls("Service interface and service cannot be null", serviceInterface, service);


        final String serviceName =  camelCaseLower(underBarCase(serviceInterface.getSimpleName()));


        doRegisterServiceCall(false, serviceName, service, Conversions.array(String.class,
                classMeta(serviceInterface).instanceMethods()));

    }

    /** Allows subclasses to register one way service calls.
     *
     * @param serviceName name of service used to lookup mail box
     * @param service actual service object
     * @param methods methods names we are exposing for invocation. Used for mail box lookup and mail box registry.
     */
    public void registerOneWayServiceCalls(String serviceName, Object service, String... methods) {


        doRegisterServiceCall(true, serviceName, service, methods);
    }



    /** Helper method to create a request / reply handler from a service.
     *
     * @param serviceName name of service used to lookup mail box
     * @param service service we are calling.
     * @param messageQueue the full name of the message queue
     * @param method the method we are invoking.
     */
    private void registerServiceInvoker(final String serviceName, final Object service, final String messageQueue,
                                        final MethodAccess method) {

        logger.debug("request reply : message queue listener registered", messageQueue,
                "for service", service, "for service method", method.name());

        mailBox.registerHandler(messageQueue, new Handler<Message>() {
            @Override
            public void handle(Message event) {

                if (debug) {
                    logger.debug("BEGIN MESSAGE RECEIVED\n", multiply('_', 50), multiply('\n', 5));
                }
                handleRequestReply(event, serviceName, service, messageQueue, method);
            }
        });
    }

    /**
     * Handles a request reply call.
     * @param event message from bus
     * @param serviceName name of service used to lookup mail box
     * @param service service we are calling.
     * @param messageQueue the full name of the message queue
     * @param method reflection method used to invoke method
     */
    private void handleRequestReply(Message event, String serviceName,
                                    Object service, String messageQueue,
                                    MethodAccess method) {


        beforeHandleRequestReply(event, serviceName, messageQueue);

        if ( debug ) {
            dieIfAnyParametersAreNull("MailBoxServiceDispatch::handleRequestReply",
                    event, serviceName, service, messageQueue, method);
        }

        String result;
        Object listOrMapArg;
        Object returnValue;
        boolean parsedOk = false;

        try {

            listOrMapArg = fromJson(event.body());
            parsedOk = true;
            returnValue = invokeMethodFromObjectArg(service, method, listOrMapArg);
            result = toJson(returnValue);

        } catch (Exception ex) {
            if (debug) {
                ex.printStackTrace();
                logger.debug(ex, "PROBLEM RUNNING handleRequestReply",  serviceName,
                        messageQueue,
                        method);
            }
            CharBuf buf = CharBuf.create(255);
            buf.add(ResponseUtils.ERROR_MARKER).add("\n\n");
            buf.add(toJson(map(

                    "event", event,
                    "serviceName", serviceName,
                    "service", Str.toString(service),
                    "messageQueue", messageQueue,
                    "methodName", method==null ? "method was null" : method.name(),
                    "methodDeclaringClass", method==null ? "method was null" : method.declaringType()
            ))).add(',');
            buf.add(Exceptions.asJson(ex));
            buf.add(",\n\n\"ERROR\"]");
            result = buf.toString();

            if (debug) {
                handleErrorFromMethodInvoke(parsedOk, event, serviceName, messageQueue, method, ex);
            }
        }
        event.reply(result);

        afterHandleRequestReply(event, serviceName, messageQueue);

    }

    protected  void afterHandleRequestReply(Message event, String serviceName, String messageQueue) {
    }

    protected void beforeHandleRequestReply(Message event, String serviceName, String messageQueue) {
    }


    /**
     * Handles one way calls. Calls that do not expect returns to be sent.
     * I put some extra care in error handling as there is no other way to know if the one way message failed except the log.
     * @param event message from bus
     * @param serviceName name of service used to lookup mail box
     * @param service service we are calling.
     * @param messageQueue the full name of the message queue
     * @param method reflection method used to invoke method
     */
    private void handleOneWay(Message event, String serviceName, Object service, String messageQueue, MethodAccess method) {


        Object listOrMapArg;
        boolean parsedOk = false;
        try {
            listOrMapArg = fromJson(event.body());
            parsedOk = true;
            invokeMethodFromObjectArg(service, method, listOrMapArg);

        } catch (Exception ex) {
            handleErrorFromMethodInvoke(parsedOk, event, serviceName, messageQueue, method, ex);
        }


    }

    /**
     * Extra error handling.
     * @param parsedOk were we able to parse
     * @param event message from bus
     * @param serviceName name of service used to lookup mail box
     * @param messageQueue the full name of the message queue
     * @param method reflection method used to invoke method
     * @param ex Exception that we recieved while trying to invoke.
     */
    private void handleErrorFromMethodInvoke(boolean parsedOk, Message event, String serviceName, String messageQueue, MethodAccess method, Exception ex) {
        logger.fatal(ex, "ERROR HANDLE_ONE_WAY", multiply('_', 50), multiply('\n', 5));

        logger.fatal("ERROR HANDLE_ONE_WAY\n", "parsed ok?", parsedOk, "serviceName", serviceName,
                "messageQueue", messageQueue, "method", method, event, "\n\n");

        if (method != null) {

            logger.fatal("ERROR HANDLE_ONE_WAY METHOD_INFO\n", method.name(), method.parameterTypes());
        }

        if (event != null) {

            logger.fatal("ERROR HANDLE_ONE_WAY\n", messageQueue, "\nEVENT BODY\n", event.body());
        }

        logger.fatal("END ERROR HANDLE_ONE_WAY", multiply('_', 50), multiply('\n', 5));
    }


    /** Helper method to create a one way handler to a service.
     *
     * @param serviceName name of service used to lookup mail box
     * @param service service we are calling.
     * @param messageQueue the full name of the message queue
     * @param method reflection method used to invoke method
     */
    private void registerOneWayServiceInvoker(final String serviceName, final Object service,
                                              final String messageQueue, final MethodAccess method) {

        if ( logger.debugOn() ) {
            logger.debug ("one way : message queue listener registered", messageQueue);
        }

        mailBox.registerHandler(messageQueue, new Handler<Message>() {
            @Override
            public void handle(Message event) {

                handleOneWay(event, serviceName, service, messageQueue, method);

            }
        });
    }

    /**
     * Helper method to verify proper usage of class.
     * This checks to see if the service object responds to these messages.
     *
     * @param serviceName name of service used to lookup mail box
     * @param service service we are calling.
     * @param methods name of methods to validate
     */
    private void validateMethods(String serviceName, Object service, String[] methods) {
        for (String method : methods) {
            if (!respondsTo(service, method)) {
                die("Service name registry failed", serviceName,
                        "for method", method, "for object", service, "with methods", methods);
            }
        }
    }




    /** Handles the actual service call registration.
     *
     * @param oneWay is this a one way call or not
     * @param serviceName name of service used to lookup mail box
     * @param service service we are calling.
     * @param methods methods names we are exposing for invocation. Used for mail box lookup and mail box registry.
     */
    public void doRegisterServiceCall (final boolean oneWay,  final String serviceName,
                                       final Object service, final String... methods) {



        if (logger.debugOn()) logger.debug("ServiceName", serviceName, methods);

        /** Preconditions. */

        //No null args
        requireNonNulls("No null as arguments", serviceName, service, methods);

        //Some methods needed
        if ( methods.length == 0 ) {
            die("Methods cannot be empty", serviceName, service, methods);
        }

        //Object supports method names
        validateMethods(serviceName, service, methods);

        /** End preconditions. */


        /* Code */
        ClassMeta classMeta = classMeta(service.getClass());

        /** Register each method as a mailbox queue. */
        for (final String methodName : methods) {
            /** Name of message Queue. */
            final String messageQueue = join('.', serviceName, methodName);

            /**
             * Service method that we are going to invoke.
             */
            final MethodAccess method = classMeta.method(methodName);

            /** Register the method call. */
            if (!oneWay) {
                registerServiceInvoker( serviceName, service, messageQueue, method );
            } else {
                registerOneWayServiceInvoker( serviceName, service, messageQueue, method );
            }

        }

    }



    protected String toJson( Object object ) {
        return jsonSerializer.serialize(object).toString();
    }


    protected Object fromJson( String json ) {
        return jsonParser.parse( json);
    }

}
