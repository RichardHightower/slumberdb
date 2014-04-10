package info.slumberdb.mailbox;

import java.util.Map;

/**
 * Created by Richard on 4/6/14.
 * Message from a mailbox.
 */
public interface Message {

    void reply(String response);

    String body();

    Map<String, Object> headers();

}
