package info.slumberdb.rest;

import org.boon.core.Handler;


/**
 * Handle a request.
 */
public interface RestHandler {


    void handle( final Request request, Handler<Response> responseHandler) ;

}

