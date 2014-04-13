package info.slumberdb;


import org.boon.HTTP;

import static org.boon.Boon.puts;

/**
 * Created by Richard on 4/8/14.
 */
public class RestClient {

    public static void main(String... args) throws Exception {

        HTTP.postJSON("http://localhost:8082/slumberdb/client/put", "{\n" +
                "    \"key\": \"bob\",\n" +
                "    \"value\": \"foo\"\n" +
                "}");

        puts(HTTP.postJSON("http://localhost:8082/slumberdb/client/load", "\"bob\""));

    }
}
