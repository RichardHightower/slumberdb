package info.slumberdb.rest;

/**
 * Created by Richard on 4/6/14.
 * Response object represents an HTTP response.
 * It packages the JSON body and the HTTP code that needs to be written out.
 */
public class Response {


    /**
     * Body of the response.
     * This is the JSON that is getting sent to the client.
     */
    private String response;

    /**
     * HTTP status code if using HTTP.
     */
    private int code;

    /**
     * Constructor for creating responses.
     *
     * @param response body of the response usually JSON
     * @param code     HTTP status code
     */
    private Response(String response, int code) {
        this.response = response;
        this.code = code;
    }

    /**
     * <p>
     * Create a response object.
     * If the response String starts with ResponseUtils.ERROR_MARKER
     * then it is an error message and the status code is changed to 500.
     * </p>
     *
     * @param response the body of the response
     * @return a response
     */
    public static Response response(String response) {

        //http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
        if (response.startsWith(ResponseUtils.ERROR_MARKER)) {
            return new Response(response, 500);
        } else {
            return new Response(response, 200);
        }
    }

    /**
     * <p>
     * Used to create a response with a certain response code.
     * </p>
     *
     * @param response response
     * @param code     the code
     * @return the response
     */
    public static Response response(String response, int code) {
        return new Response(response, code);
    }

    /**
     * access JSON response
     *
     * @return the response
     */
    public String response() {
        return response;
    }

    /**
     * Access code
     *
     * @return the code
     */
    public int code() {
        return code;
    }

}
