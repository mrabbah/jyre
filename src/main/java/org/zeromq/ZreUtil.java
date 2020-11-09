package org.zeromq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rabbah
 */
public class ZreUtil {

    public static int convertToInt(byte b1, byte b2, byte b3, byte b4) {
        byte[] byteArray = new byte[]{b1, b2, b3, b4};
        ByteBuffer byteArrayWrapped = ByteBuffer.wrap(byteArray);
        return byteArrayWrapped.getInt();
    }

    public static int getNumber1(ByteBuffer buffer) {
        return convertToInt((byte)0, (byte)0, (byte)0, buffer.get());
    }
    
    public static void setNumber1(ByteArrayOutputStream baos, int number) throws IOException {
        byte[] numberBytes = ByteBuffer.allocate(4).putInt(number).array();
        baos.write(numberBytes[3]);
    }
    
    public static int getNumber2(ByteBuffer buffer) {
        return convertToInt((byte)0, (byte)0, buffer.get(), buffer.get());
    }
    
    public static void setNumber2(ByteArrayOutputStream baos, int number) throws IOException {
        byte[] bytes = new byte[2];     
        byte[] numberBytes = ByteBuffer.allocate(4).putInt(number).array();
        bytes[0] = numberBytes[2];
        bytes[1] = numberBytes[3];
        baos.write(bytes);
    }
    
    public static int getNumber4(ByteBuffer buffer) {
        return convertToInt(buffer.get(), buffer.get(), buffer.get(), buffer.get());
    }
    
    public static void setNumber4(ByteArrayOutputStream baos, int number) throws IOException {
        byte[] bytes = new byte[4];     
        byte[] numberBytes = ByteBuffer.allocate(4).putInt(number).array();
        bytes[0] = numberBytes[0];
        bytes[1] = numberBytes[1];
        bytes[2] = numberBytes[2];
        bytes[3] = numberBytes[3];
        baos.write(bytes);
    }
    
    public static String getString(ByteBuffer buffer) {
        int stringLenght = getNumber1(buffer);
        byte[] stringBytes = new byte[stringLenght];
        buffer.get(stringBytes);
        String str = new String(stringBytes, ZMQ.CHARSET);
        return str;
    }
    
    public static void setString(ByteArrayOutputStream baos, String string) throws IOException {
        int stringLenght = string.length();
        setNumber1(baos, stringLenght);
        byte[] stringBytes = string.getBytes(ZMQ.CHARSET);
        baos.write(stringBytes);
    }
    
    public static UUID getUUID(ByteBuffer buffer) {
        byte[] bytes = new byte[16];
        buffer.get(bytes);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }
    
    public static void setUUID(ByteArrayOutputStream baos, UUID uuid) throws IOException {
        ByteBuffer uuidBuffer = ByteBuffer.wrap(new byte[16]);
        uuidBuffer.putLong(uuid.getMostSignificantBits());
        uuidBuffer.putLong(uuid.getLeastSignificantBits());
        baos.write(uuidBuffer.array());
    }
    
    public static String[] getStrings(ByteBuffer buffer) {
        int strNb = getNumber4(buffer);
        String[] strs = new String[strNb];
        for (int i = 0; i < strNb; i++) {
            int strLenght = getNumber4(buffer);
            byte[] strBytes = new byte[strLenght];
            buffer.get(strBytes);
            strs[i] = new String(strBytes, ZMQ.CHARSET);
        }
        return strs;
    }
    
    public static void setStrings(ByteArrayOutputStream baos, String[] strs) throws IOException {
        int strNb = strs.length;
        setNumber4(baos, strNb);
        for (String str : strs) {
            setLongString(baos, str);         
        }
    }
    public static String getLongString(ByteBuffer buffer) {
        int stringLenght = getNumber4(buffer);
        byte[] stringBytes = new byte[stringLenght];
        buffer.get(stringBytes);
        String str = new String(stringBytes, ZMQ.CHARSET);
        return str;
    }
    
    public static void setLongString(ByteArrayOutputStream baos, String str) throws IOException {
        int strLenght = str.length();
        setNumber4(baos, strLenght);
        byte[] stringBytes = str.getBytes(ZMQ.CHARSET);
        baos.write(stringBytes);
    }
    
    public static Map<String, String> getHeaders(ByteBuffer buffer) {
        Map<String, String> headers = new HashMap<>();
        int headersLenght = ZreUtil.getNumber4(buffer);
        for (int i = 0; i < headersLenght; i++) {
            String key = getString(buffer);
            String val = getLongString(buffer);
            headers.put(key, val);
        }
        return headers;
    }
    
    public static void setHeaders(ByteArrayOutputStream baos, Map<String, String> headers) throws IOException {
        int headersLenght = headers.size();
        setNumber4(baos, headersLenght);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            setString(baos, key);
            setLongString(baos, val);
        }
    }
    
    public static byte[] getPrefix(ByteBuffer buffer) {
        byte[] prefix = new byte[ZreConstants.DEFAULT_PREFIX.length];
        buffer.get(prefix);
        return prefix;
    }
    
    public static void setPrefix(ByteArrayOutputStream baos) throws IOException {
        baos.write(ZreConstants.DEFAULT_PREFIX);
    }
    
    public static byte[] getMessage(ByteBuffer buffer) {
        byte[] msg = new byte[buffer.remaining()];
        buffer.get(msg);
        return msg;
    }
    
    public static void setMessage(ByteArrayOutputStream baos, byte[] msg) throws IOException {
        baos.write(msg);
    }
}
