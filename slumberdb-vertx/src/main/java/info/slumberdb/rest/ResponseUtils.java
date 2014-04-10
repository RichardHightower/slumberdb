package info.slumberdb.rest;

import org.boon.core.Handler;
import org.boon.core.Type;
import org.boon.core.reflection.BeanUtils;
import org.boon.core.reflection.fields.FieldAccess;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.boon.json.serializers.impl.JsonSimpleSerializerImpl;
import org.boon.primitive.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.boon.Boon.puts;
import static org.boon.Boon.sputs;
import static org.boon.Maps.map;

/**
 * Created by Richard on 4/6/14.
 * Utility methods to create responses.
 */
public class ResponseUtils {

    /**
     * OK JSON message that we output.
     * This is for endpoints that don't really have a real response.
     */
    public static final String JSON_OK = "[\"ok\"]";
    /* Marking used to denote error messages from message service. Responses that begin with this are exceptions.
           This is needed to change the HTTP response code from 200 to something more meaningful.
         */
    public static final String ERROR_MARKER = "[\"ERROR\",";


    /**
     * Handles exceptions.
     *
     * @param request         the request that threw the exception
     * @param ex              the exception
     * @param mapper          JSON object mapper to turn the exception into JSON.
     * @param responseHandler
     */
    public static void handleException(final Request request, final Exception ex, final JsonSimpleSerializerImpl mapper, Handler<Response> responseHandler) {
        String json = mapper.serialize(
                map(
                        "message", sputs("Unexpected Exception for METHOD", request.method(), "FOR URI", request.path()),
                        "path", request.path(),
                        "params", request.params(),
                        "error", ex.getMessage()
                )
        ).toString();

        responseHandler.handle(Response.response(toJSON(ex, json), 500));
    }


    /**
     * Converts an exception into a JSON response.
     *
     * @param ex          the exception
     * @param requestJson the JSON response.
     * @return JSON version of exception
     */
    public static String toJSON(Exception ex, String requestJson) {
        ByteBuf buffer = ByteBuf.create(255);
        buffer.addByte('{');


        buffer.add("\n    ").addJSONEncodedString("request").add(" : ")
                .add(requestJson).add(",\n");

        buffer.add("\n    ").addJSONEncodedString("message").add(" : ")
                .addJSONEncodedString(ex.getMessage()).add(",\n");

        buffer.add("    ").addJSONEncodedString("localizedMessage").add(" : ")
                .addJSONEncodedString(ex.getLocalizedMessage()).add(",\n");

        buffer.add("    ").addJSONEncodedString("stackTrace").add(" : ")
                .addByte('[').addByte('\n');

        final StackTraceElement[] stackTrace = ex.getStackTrace();

        for (int index = 0; index < (stackTrace.length > 10 ? 10 : stackTrace.length); index++) {
            StackTraceElement element = stackTrace[index];
            if (index != 0) {
                buffer.addByte(',');
                buffer.addByte('\n');
            }
            index++;
            buffer.add("           { ");
            buffer.add("             ").addJSONEncodedString("className").add(" : ")
                    .addJSONEncodedString(element.getClassName()).add(",\n");

            buffer.add("             ").addJSONEncodedString("methodName").add(" : ")
                    .addJSONEncodedString(element.getMethodName()).add(",\n");

            buffer.add("             ").addJSONEncodedString("lineNumber").add(" : ")
                    .add("" + element.getLineNumber()).add("}\n");

        }

        buffer.add("\n    ]\n}");
        return buffer.toString();

    }


    /* ------------------------------------ */

    /*
     * Everything below this line is used for documentation or debugging.
     * It has no bearing on runtime system or operations of runtime systems.
     */

    /* ------------------------------------ */


    /**
     * Used to display meta data for end points.
     * This is used for debugging and documentation only.
     * It is not a runtime feature.
     *
     * @param postHandlers map of handlers to display meta-data about.
     * @return JSON to display that contains meta-data about the postHandlers.
     */
    public static String extractPublicAPI(Map<String, RequestBinding> postHandlers) {

        ObjectMapper mapper = JsonFactory.create();
        Set<Map.Entry<String, RequestBinding>> entries = postHandlers.entrySet();
        List<Map<String, Object>> list = new ArrayList<>();

        for (Map.Entry<String, RequestBinding> entry : entries) {

            String name = entry.getKey();
            Class<?> bodyType = entry.getValue().bodyType();
            Class<?> returnType = entry.getValue().returnType();

            puts("name", name);
            ResponseUtils.extractAPIJSONMetaDataForClientAPIs(list, name, bodyType, returnType);

        }

        return mapper.toJson(list);

    }

    /**
     * Used to quickly display end points exposed.
     *
     * @param list
     * @param name
     * @param bodyType
     * @param returnType
     */
    public static void extractAPIJSONMetaDataForClientAPIs(List<Map<String, Object>> list,
                                                           Object name, Class<?> bodyType,
                                                           Class<?> returnType) {

        Object results = null;
        Object request = null;

        if (returnType != null) {
            if (returnType.isPrimitive()) {

                results = map(
                        "name", name,
                        "typeName", returnType.getName(),
                        "type", Type.getType(returnType)
                );

            } else if (returnType.isArray()) {

                results = map(
                        "name", name,
                        "typeName", returnType.getName(),
                        "type", Type.getType(returnType),
                        "componentType", Type.getType(returnType.getComponentType())
                );

            } else {

                results = map(
                        "name", name,
                        "typeName", returnType.getSimpleName(),
                        "type", Type.getType(returnType),
                        "fields", getFields(returnType)
                );
            }
        }

        if (bodyType != null) {

            request = map(
                    "name", name,
                    "typeName", bodyType.getSimpleName(),
                    "type", Type.getType(bodyType),
                    "fields", getFields(bodyType)
            );
        }

        list.add(
                map(
                        "request", request,
                        "results", results
                )
        );
    }

    /**
     * Helper method to extract meta-data
     * <p/>
     * Used to display meta data for end points.
     * This is used for debugging and documentation only.
     * It is not a runtime feature.
     *
     * @param type
     * @return
     */
    private static List<Map<String, Object>> getFields(Class<?> type) {

        Map<String, FieldAccess> fieldsFromObject = BeanUtils.getFieldsFromObject(type);

        List<Map<String, Object>> fieldList = new ArrayList<>();

        for (FieldAccess field : fieldsFromObject.values()) {

            if (field.isStatic()) continue;

            if (!(field.typeEnum() == Type.INSTANCE)) {

                if (field.typeEnum().isCollection()) {

                    Type componentType = Type.getType(field.getComponentClass());
                    if (componentType == Type.INSTANCE && field.getComponentClass() != null) {

                        extractCollectionComponentTypeInfo(fieldList, field, componentType);
                    } else {
                        extractBasicComponentTypeInfo(fieldList, field, componentType);

                    }
                } else if (field.typeEnum() == Type.ENUM) {

                    fieldList.add(
                            map(
                                    "name", (Object) field.name(),
                                    "type", Type.getType(field.type()),
                                    "typeName", field.type().getSimpleName(),
                                    "possibleValues", field.type().getEnumConstants()
                            )
                    );

                } else {
                    fieldList.add(
                            map(
                                    "name", (Object) field.name(),
                                    "type", Type.getType(field.type())
                            )
                    );

                }
            } else {
                extractAPIJSONMetaDataForClientAPIs(fieldList, field.name(), field.type(), null);
            }
        }
        return fieldList;
    }

    /**
     * Used to display meta data for end points.
     * This is used for debugging and documentation only.
     * It is not a runtime feature.
     *
     * @param fieldList
     * @param field
     * @param componentType
     */
    private static void extractBasicComponentTypeInfo(List<Map<String, Object>> fieldList, FieldAccess field, Type componentType) {
        if (componentType == Type.ENUM) {
            fieldList.add(
                    map(
                            "name", (Object) field.name(),
                            "type", field.typeEnum(),
                            "componentType", componentType,
                            "componentTypeName", field.getComponentClass().getSimpleName(),
                            "possibleValues", field.getComponentClass().getEnumConstants()
                    )
            );
        } else {

            fieldList.add(
                    map(
                            "name", (Object) field.name(),
                            "type", field.typeEnum(),
                            "componentType", componentType
                    )
            );
        }
    }

    /**
     * Used to display meta data for end points.
     * This is used for debugging and documentation only.
     * It is not a runtime feature.
     *
     * @param fieldList
     * @param field
     * @param componentType
     */
    private static void extractCollectionComponentTypeInfo(List<Map<String, Object>> fieldList, FieldAccess field, Type componentType) {


        fieldList.add(
                map(
                        "name", field.name(),
                        "type", field.typeEnum(),
                        "componentType", componentType,
                        "componentTypeName", field.getComponentClass().getSimpleName(),
                        "componentFields", getFields(field.getComponentClass())
                )
        );
    }


}

