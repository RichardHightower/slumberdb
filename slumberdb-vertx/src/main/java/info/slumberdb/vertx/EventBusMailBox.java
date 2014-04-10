package info.slumberdb.vertx;

import info.slumberdb.mailbox.MailBox;
import info.slumberdb.mailbox.Message;
import org.boon.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;

import java.util.Map;

/**
 * Event bus in case we want to tap into messaging (audit trail) or use a different
 * provider (RabbitMQ, ActiveMQ).
 *
 * @author Rick Hightower
 */
public class EventBusMailBox implements MailBox {

    /**
     * The actual event bus.
     */
    private EventBus eventBus;

    /**
     * Event bus wrapper.
     *
     * @param eventBus event bus
     */
    public EventBusMailBox(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Register a message handler.
     *
     * @param address address to send the message, i.e., scoreService.updatePreferences
     * @param handler message handler
     */
    @Override
    public void registerHandler(String address, final Handler<Message> handler) {
        eventBus.registerHandler(address, new org.vertx.java.core.Handler<org.vertx.java.core.eventbus.Message>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message event) {

                /** If the body is a Buffer then wrap it in a CommunicationBuffer so we can read the message map. */
                Object body = event.body();
                if (body instanceof Buffer) {
                    Buffer buffer = (Buffer) body;
                    CommunicationBuffer wrapper = new CommunicationBuffer(buffer);
                    Map<String, Object> map = wrapper.readMap();
                    handler.handle(new VertxMessage(map, event));
                } else {
                    handler.handle(new VertxMessage(event));
                }
            }
        });
    }

    /**
     * Publish a String message.
     *
     * @param address address to send the message, i.e., someService.someMethod
     * @param message message handler
     */
    @Override
    public void publish(String address, String message) {
        eventBus.publish(address, message);
    }

    /**
     * Send a string message.
     *
     * @param address address to send the message, i.e., someService.someMethod
     * @param message
     */
    @Override
    public void send(String address, String message) {
        eventBus.send(address, message);

    }


    /**
     * Publish a string message to a mailbox address.
     * Creates a simple map message.
     *
     * @param address     address to send the message, i.e., scoreService.updatePreferences
     * @param headers     headers
     * @param messageBody message body
     */
    public void publish(String address, Map<String, Object> headers, String messageBody) {

        Buffer buffer = createBufferFromMap(headers, messageBody);
        eventBus.publish(address, buffer);
    }

    /**
     * Creates a map message using the headers and the message body.
     *
     * @param address     address to send the message, i.e., someService.someMethod
     * @param headers     headers
     * @param messageBody message body
     */
    public void send(String address, Map<String, Object> headers, String messageBody) {

        Buffer buffer = createBufferFromMap(headers, messageBody);
        eventBus.send(address, buffer);

    }


    /**
     * Used to send simple key / values pairs in vertx.
     *
     * @param map  map to send (usually a header)
     * @param body body of message
     * @return return type.
     */
    private Buffer createBufferFromMap(Map<String, Object> map, String body) {

        Buffer buffer = new Buffer();
        CommunicationBuffer wrapper = new CommunicationBuffer(buffer);

        map.put("body", body);

        wrapper.addMap(map);


        return buffer;
    }

    /**
     * Send a request that expects a reply message.
     *
     * @param address      address to send the message, i.e., someService.someMethod
     * @param message      message
     * @param replyHandler reply handler
     */
    @Override
    public void requestReply(String address, String message, final Handler<Message> replyHandler) {
        eventBus.send(address, message, new org.vertx.java.core.Handler<org.vertx.java.core.eventbus.Message<String>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<String> event) {
                replyHandler.handle(new VertxMessage(event));
            }
        });
    }

    /**
     * Create a message using map and body and expect a similar message in response.
     *
     * @param address      address to send the message, i.e., someService.someMethod
     * @param headers      headers
     * @param messageBody  message body
     * @param replyHandler reply handler
     */
    @Override
    public void requestReply(String address, Map<String, Object> headers, String messageBody,
                             final Handler<Message> replyHandler) {

        final Buffer buffer = createBufferFromMap(headers, messageBody);
        eventBus.send(address, buffer, new org.vertx.java.core.Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> event) {
                replyHandler.handle(new VertxMessage(event));
            }
        });

    }
}
