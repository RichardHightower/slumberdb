package info.slumberdb.vertx;

import org.boon.core.Type;
import org.vertx.java.core.buffer.Buffer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.boon.Exceptions.die;
import static org.boon.core.Conversions.toEnum;

/**
 * Buffer to send messages to the backend.
 *
 * @author Rick Hightower
 */
public class CommunicationBuffer {


    /**
     * The actual message that we are wrapping
     */
    private final Buffer buffer;

    /**
     * Position in the buffer.
     */
    private int pos;

    /**
     * Wrapping a vertx buffer.
     *
     * @param buffer
     */
    public CommunicationBuffer(Buffer buffer) {
        this.buffer = buffer;
    }


    /**
     * Set the position.
     */
    public void setPos(int pos) {
        this.pos = pos;
    }


    /**
     * Add a string to the buffer.
     * <p/>
     * We add the length as an int and then append a string.
     *
     * @param string string to add
     */
    public void addString(String string) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        buffer.appendInt(bytes.length);
        buffer.appendBytes(bytes);
    }


    /**
     * Read a string from the buffer.
     * Read the length and then use getBytes to read the string, then convert it to a UTF-8 string.
     * This increments the position.
     *
     * @return
     */
    public String readString() {
        int length = readInt();
        byte[] bytes = buffer.getBytes(this.pos, this.pos + length);
        pos += length;
        return new String(bytes, StandardCharsets.UTF_8);
    }


    /**
     * Read a boolean.
     *
     * @return boolean
     * <p/>
     * Updates position by 1.
     */
    public boolean readBoolean() {
        byte i = readByte();

        return i == 0 ? false : true;
    }


    /**
     * Read a single byte.
     *
     * @return single byte.
     * <p/>
     * Updates position.
     */
    public byte readByte() {
        byte i = buffer.getByte(pos);
        pos += 1;
        return i;
    }


    /**
     * Read a short.
     *
     * @return short.
     * <p/>
     * Updates position.
     */
    public short readShort() {
        short i = buffer.getShort(pos);
        pos += 2;
        return i;
    }


    /**
     * Read an int.
     * Int.
     * <p/>
     * Updates position.
     *
     * @return
     */
    public int readInt() {
        int i = buffer.getInt(pos);
        pos += 4;
        return i;
    }


    /**
     * Reads a float.
     * Updates position.
     *
     * @return
     */
    public float readFloat() {
        float v = buffer.getFloat(pos);
        pos += 4;
        return v;
    }


    /**
     * Read a double.
     * Updates position.
     *
     * @return double value
     */
    public double readDouble() {
        double v = buffer.getDouble(pos);
        pos += 8;
        return v;
    }


    /**
     * Reads a long.
     * Updates position.
     *
     * @return long value
     */
    public long readLong() {
        long v = buffer.getLong(pos);
        pos += 8;
        return v;
    }

    /**
     * Reads in a MAP of values.
     *
     * @return map of basic types.
     */
    public Map<String, Object> readMap() {
        int i = readInt();

        if (i != Type.MAP.ordinal()) {
            die("Current location is not a map location = ", pos);
        }

        final int size = readInt();
        int iType;
        Type type;
        String key;
        Object value;

        Map<String, Object> map = new HashMap<>();

        for (int index = 0; index < size; index++) {
            key = readString();
            iType = readInt();
            type = toEnum(Type.class, iType);

            switch (type) {

                case BYTE_WRAPPER:
                    value = readByte();
                    break;

                case SHORT_WRAPPER:
                    value = readShort();
                    break;

                case INTEGER_WRAPPER:
                    value = readInt();
                    break;

                case FLOAT_WRAPPER:
                    value = readFloat();
                    break;

                case CHAR_SEQUENCE:
                    value = readString();
                    break;


                case STRING:
                    value = readString();
                    break;

                case DOUBLE_WRAPPER:
                    value = readDouble();
                    break;


                case BOOLEAN_WRAPPER:
                    value = readBoolean();
                    break;

                default:
                    value = die(Object.class, "Unrecognized type", type, "key", key);

            }

            map.put(key, value);
        }

        return map;

    }

    /**
     * Add a map of name value pairs of basic types to the buffer.
     *
     * @param map map
     */
    public void addMap(Map<String, Object> map) {

        buffer.appendInt(Type.MAP.ordinal());
        buffer.appendInt(map.size());

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            this.addString(entry.getKey());

            Type instanceType = Type.getInstanceType(entry.getValue());

            buffer.appendInt(instanceType.ordinal());

            switch (instanceType) {


                case BOOLEAN_WRAPPER:
                    boolean value = (Boolean) entry.getValue();
                    buffer.appendByte(value ? (byte) 1 : (byte) 0);
                    break;

                case BYTE_WRAPPER:
                    buffer.appendInt((Byte) entry.getValue());
                    break;

                case SHORT_WRAPPER:
                    buffer.appendInt((Short) entry.getValue());
                    break;

                case INTEGER_WRAPPER:
                    buffer.appendInt((Integer) entry.getValue());
                    break;

                case FLOAT_WRAPPER:
                    buffer.appendFloat((Float) entry.getValue());
                    break;

                case CHAR_SEQUENCE:
                    this.addString(entry.getValue().toString());
                    break;


                case STRING:
                    this.addString((String) entry.getValue());
                    break;

                case DOUBLE_WRAPPER:
                    buffer.appendDouble((Double) entry.getValue());
                    break;

            }

        }


    }

}