package info.slumberdb.integration;

import info.slumberdb.vertx.RestVerticle;
import org.boon.HTTP;
import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.testtools.TestVerticle;

import static org.boon.Boon.puts;
import static org.boon.Exceptions.die;
import static org.boon.Str.rpad;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Simple integration test which shows tests deploying other verticles, using the Vert.x API etc
 */
public class BasicIntegration extends TestVerticle {

    private String lastResponse;
    private boolean ok;

    public void getHTTP(String uri, final Handler<String> handler) {

        container.deployVerticle(RestVerticle.class.getName());

        try {
            synchronized (this) {
                wait(500);
            }
        } catch (InterruptedException e) {

        }


        String str = HTTP.get(uri);
        handler.handle(str);
    }


    public void postHTTP(String uri, String body, final Handler<String> handler) {

        container.deployVerticle(RestVerticle.class.getName());

        try {
            synchronized (this) {
                wait(500);
            }
        } catch (InterruptedException e) {

        }


        String str = HTTP.postJSON(uri, body);
        handler.handle(str);
    }


    public boolean GET(final String message, final String uri, String params) {
        return _GET(message, "slumberdb", uri, 8082, params);
    }


    public boolean _GET(final String message, final String path,
                        final String uri, int port, String params) {

        String url = "http://localhost:" + port + "/" + path + "/" + uri + "?" + params;

        puts("HTTP REST ENDPOINT");
        puts(rpad("description", 22), ":", message);
        puts(rpad("URI", 22), ":", uri);
        puts(rpad("parameters", 22), ":", params);
        puts(rpad("URL", 22), ":", url.replace("localhost", "10.198.666.66"));

        puts("--------------------------------------------");

        try {
            getHTTP(url,
                    new Handler<String>() {
                        @Override
                        public void handle(String response) {
                            lastResponse = response;
                            puts("RESPONSE\n", response);
                            testComplete();
                        }
                    }
            );
            return true;
        } catch (Exception ex) {
            puts(ex.getMessage());
            ex.printStackTrace();
            testComplete();
            return false;
        }

    }


    public boolean POST(final String message, final String uri,
                        String params, String body) {
        return _POST(message, "slumberdb", uri, 8082, params, body);
    }


    public boolean _POST(final String message, final String path, final String uri, int port, String params, String body) {

        String url = "http://localhost:" + port + "/" + path + "/" + uri + "?" + params;

        puts("HTTP REST ENDPOINT");
        puts(rpad("description", 22), ":", message);
        puts(rpad("URI", 22), ":", uri);
        puts(rpad("parameters", 22), ":", params);
        puts(rpad("URL", 22), ":", url.replace("localhost", "10.198.192.66"));

        puts("--------------------------------------------");
        puts(rpad("BODY", 22), ":", "\n");

        puts(body);
        puts("--------------------------------------------");

        try {
            postHTTP(url, body,
                    new Handler<String>() {
                        @Override
                        public void handle(String response) {
                            lastResponse = response;
                            puts("RESPONSE\n", response);
                            testComplete();
                        }
                    }
            );
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            testComplete();
            return false;
        }

    }


    @Test
    public void testPut() {

        String body = "{\n" +
                "    \"key\": \"bob\",\n" +
                "    \"value\": \"foo\"\n" +
                "}";


        ok = POST("post put", "client/put", "test=1", body) || die(lastResponse);

        puts("last response", lastResponse);
    }


    @Test
    public void testGet() {

        String body = "\"bob\"";


        ok = POST("post put", "client/load", "test=1", body) || die(lastResponse);

        puts("last response", lastResponse);
    }


}
