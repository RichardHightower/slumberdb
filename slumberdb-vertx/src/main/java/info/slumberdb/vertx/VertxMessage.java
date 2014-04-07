package info.slumberdb.vertx;


import info.slumberdb.mailbox.Message;
import org.vertx.java.core.buffer.Buffer;

import java.util.Collections;
import java.util.Map;

import static org.boon.Exceptions.die;

/**
 *
 * Vertx message used to emulate Message.
 * This allows a simple string message or a message that allows a Map and a String body.
 *
 * @author Rick Hightower
 */
public class VertxMessage implements Message {

    /**
     * Holds the internal Vertx message.
     */
    private transient org.vertx.java.core.eventbus.Message<?> internalMessage;

    /**
     * The body of the message.
     */
    final String body;

    /**
     * Headers for the message.
     */
    final Map<String, Object> headers;

    /**
     * Vertx message.
     *
     * If the body is a string, assume a string message if a buffer assume a Map message.
     *
     * @param internalMessage message we are wrapping
     */
    public VertxMessage(org.vertx.java.core.eventbus.Message<?> internalMessage) {

        this.internalMessage = internalMessage;

        Object objBody = this.internalMessage.body();
        if (objBody instanceof String) {
            body = (String) objBody;
            headers = Collections.EMPTY_MAP;
        } else if (objBody instanceof Buffer){
            CommunicationBuffer communicationBuffer = new CommunicationBuffer((Buffer) objBody);
            Map<String, Object> map = communicationBuffer.readMap();
            this.body = (String) map.get("body");
            this.headers = map;
            map.remove("body");
        } else {
            die("Body must be Buffer or String");
            body = null;
            headers = null;
        }
    }


    /**
     *
     * @param headers headers
     * @param internalMessage internal message
     */
    public VertxMessage(Map<String, Object> headers, org.vertx.java.core.eventbus.Message<?> internalMessage) {

        this.internalMessage = internalMessage;
        body = (String) headers.get("body");
        headers.remove("body");
        this.headers = headers;
    }

    /**
     * Reply to a message.
     * @param response response
     */
    @Override
    public void reply(String response) {
        internalMessage.reply(response);
    }


    /**
     * Reply to a message.
     * @param headers headers
     * @param response response
     *
     */
    public void reply(Map<String, Object> headers, String response) {

        Buffer buffer = new Buffer();
        CommunicationBuffer wrapper = new CommunicationBuffer(buffer);
        if (!headers.containsKey("body")){
            headers.put("body", response);
        }
        wrapper.addMap(headers);
        internalMessage.reply(buffer);
    }

    /**
     * Returns the body.
     * @return
     */
    @Override
    public String body() {
        return body;
    }

    /**
     * Returns message headers.
     * @return
     */
    @Override
    public Map<String, Object> headers() {
        return headers;
    }

    /**
     * See if two messages are the same.
     * @param o object
     * @return same?
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VertxMessage that = (VertxMessage) o;

        if (body != null ? !body.equals(that.body) : that.body != null) return false;
        if (internalMessage != null ? !internalMessage.equals(that.internalMessage) : that.internalMessage != null)
            return false;

        return true;
    }

    /**
     * Hashcode
     * @return
     */
    @Override
    public int hashCode() {
        int result = internalMessage != null ? internalMessage.hashCode() : 0;
        result = 31 * result + (body != null ? body.hashCode() : 0);
        return result;
    }

    /**
     * toString
     * @return string version
     */
    @Override
    public String toString() {
        return "Message{" +
                "\"body\":'" + body + '\'' +
                '}';
    }
}
