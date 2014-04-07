package info.slumberdb.vertx;

import info.slumberdb.mailbox.MailBox;
import info.slumberdb.mailbox.Message;
import org.boon.core.Handler;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * Created by Richard on 4/6/14.
 */
public class RestVerticleTest {

    RestVerticle verticle;

    @Before
    public void init() {

        RestVerticle verticle = new RestVerticle();
        verticle.init(new MailBox() {
            @Override
            public void registerHandler(String address, Handler<Message> handler) {

            }

            @Override
            public void publish(String address, String message) {

            }

            @Override
            public void publish(String address, Map<String, Object> map, String message) {

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
