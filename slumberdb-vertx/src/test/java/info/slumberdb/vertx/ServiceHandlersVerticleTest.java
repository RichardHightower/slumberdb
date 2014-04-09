package info.slumberdb.vertx;

import info.slumberdb.mailbox.MailBox;
import info.slumberdb.mailbox.Message;
import org.boon.core.Handler;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.boon.Boon.puts;

/**
 * Created by Richard on 4/6/14.
 */
public class ServiceHandlersVerticleTest {
    ServiceHandlersVerticle verticle;

    @Before
    public void init() {

        verticle = new ServiceHandlersVerticle();
        verticle.init(new MailBox() {
            @Override
            public void registerHandler(String address, Handler<Message> handler) {
                puts("registerHandler", address, handler);
            }

            @Override
            public void publish(String address, String message) {
                puts("publish", address, message);

            }

            @Override
            public void publish(String address, Map<String, Object> map, String message) {
                puts("publish", address, map, message);

            }

            @Override
            public void send(String address, String message) {

            }

            @Override
            public void send(String address, Map<String, Object> map, String message) {

            }

            @Override
            public void requestReply(String address, String message, Handler<Message> replyHandler) {

            }

            @Override
            public void requestReply(String address, Map<String, Object> map, String message, Handler<Message> replyHandler) {

            }
        });


    }

    @Test
    public void test() {

    }
}