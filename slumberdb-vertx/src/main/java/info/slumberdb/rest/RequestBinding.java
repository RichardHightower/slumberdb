package info.slumberdb.rest;

import org.boon.core.AsyncFunction;


/**
 * Binds a request to a service method.
 * Used to announce service meta-data.
 */
public class RequestBinding {

    /**
     * The actual event handler to handle the method invocation.
     */
    final AsyncFunction<Request, String> function;
    /**
     * The body type.
     */
    private final Class bodyType;
    /**
     * The response type.
     */
    private final Class returnType;

    /**
     * Creates a new request binding.
     *
     * @param bodyType   the body type
     * @param returnType the return type
     * @param function   the async function handler.
     */
    private RequestBinding(Class bodyType, Class returnType,
                           AsyncFunction<Request, String> function) {
        this.bodyType = bodyType;
        this.function = function;
        this.returnType = returnType;
    }

    /**
     * Factory method to create a body binding.
     * This is for HTTP POST messages.
     *
     * @param bodyType the type of message, Java class type that denotes the schema for the JSON payload.
     * @param function handler gets notified when a request comes in.
     * @return returns the request binding created.
     */
    public static RequestBinding bodyBinding(Class bodyType, AsyncFunction<Request, String> function) {
        return new RequestBinding(bodyType, null, function);
    }

    /**
     * Factory method to create a body binding for an HTTP POST message.
     * It has both a body and a return JSON schema.
     *
     * @param bodyType   the type of message, Java class type that denotes the schema for the JSON payload.
     * @param returnType the return type, denotes the JSON schema response as a Java class.
     * @param function   handler gets notified when a request comes in.
     * @return the request binding created.
     */
    public static RequestBinding binding(Class bodyType, Class returnType, AsyncFunction<Request, String> function) {
        return new RequestBinding(bodyType, returnType, function);
    }

    /**
     * Factory method to create an HTTP GET binding.
     * <p/>
     * This merely denotes the response schema expressed as a Java class.
     *
     * @param returnType JSON schema of the response.
     * @param function   handler gets notified when a request comes in.
     * @return the request binding created.
     */
    public static RequestBinding httpGetBinding(Class returnType, AsyncFunction<Request, String> function) {
        return new RequestBinding(Void.class, returnType, function);
    }

    /**
     * Return the bodyType for this binding.
     * Just meta data for admin.
     *
     * @return
     */
    public Class<?> bodyType() {
        return bodyType;
    }

    /**
     * Return the return type for this binding.
     * Just meta data for admin.
     *
     * @return
     */
    public Class returnType() {
        return returnType;
    }

    @Override
    public String toString() {
        return "RequestBinding{" +
                "bodyType=" + bodyType +
                ", returnType=" + returnType +
                ", function=" + function +
                '}';
    }
}
