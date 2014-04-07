package info.slumberdb.mailbox;

import org.boon.core.Handler;

import java.util.Map;

/**
 * Created by Richard on 4/6/14.
 * Mailbox to send messages.
 *
 * This is how the HTTP tier communicates with the service tier.
 */
public interface MailBox {


    /**
     * <p>
     * Register a message handler. A message handler listens for messages.
     * </p>
     *
     * @param address address to send the message, i.e., scoreService.updatePreferences
     * @param handler message handler
     */
    void registerHandler(String address, Handler<Message> handler);

    /**
     * <p>
     * Publish a message, this means there can be multiple listeners.
     * </p>
     * @param address address to send the message, i.e., scoreService.updatePreferences
     * @param message message handler
     */
    void publish(String address, String message);



    /**
     * <p>
     * Publish a message, this means there can be multiple listeners.
     * </p>
     * @param address address to send the message, i.e., scoreService.updatePreferences
     * @param message message handler
     */
    void publish(String address, Map<String, Object> map, String message);

    /**
     * <p>
     *     Sends a message.
     *     This means that just one listener is going to consume the message.
     * </p>
     * @param address
     * @param message
     */
    void send(String address, String message);



    /**
     * <p>
     *     Sends a message.
     *     This means that just one listener is going to consume the message.
     * </p>
     * @param address
     * @param map
     * @param message
     */
    public void send(String address, Map<String, Object> map, String message);


    /**
     * <p>
     *     This is to make an RPC style call.
     *     You send the message and you expect a reply to the message.
     *
     *     Request/reply.
     * </p>
     * @param address
     * @param message
     * @param replyHandler
     */
    void requestReply(String address, String message, Handler<Message> replyHandler);


    /**
     *
     * @param address
     * @param map
     * @param message
     * @param replyHandler
     */
    void requestReply(String address, Map<String, Object> map, String message, Handler<Message> replyHandler);




}
