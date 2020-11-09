package org.zeromq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rabbah
 */
public class ZreMessage {

    private String address;
    private int eventType;
    private int identity;
    private int sequence;
    private UUID uuid;
    private String endpoint;
    private String[] groups;
    private String group;
    private int status;
    private String name;
    private Map<String, String> headers;
    private byte[] msg;
    
    private static final Logger LOG = Logger.getLogger(ZreMessage.class.getName());
    
    
    private ZreMessage() {}

    public static ZreMessage unpackConnect(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.CONNECT;
        message.identity = ZreUtil.getNumber1(buffer);
        LOG.log(Level.FINE, "Identity = {0}", message.identity);
        message.uuid = ZreUtil.getUUID(buffer);
        LOG.log(Level.FINE, "From remote dealer uuid = {0}", message.uuid.toString());
        return message;
    }
    
    public static void packConnect(ByteArrayOutputStream baos, UUID uuid) throws IOException {
        ZreUtil.setNumber1(baos, 1);
        ZreUtil.setUUID(baos, uuid);
    }
    
    public static ZreMessage unpackHello(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.HELLO;
        message.sequence = ZreUtil.getNumber2(buffer);
        LOG.log(Level.FINE, "squence = {0}", message.sequence);
        message.endpoint = ZreUtil.getString(buffer);
        LOG.log(Level.FINE, "Endpoint = {0}", message.endpoint);
        message.groups = ZreUtil.getStrings(buffer);
        LOG.log(Level.FINE, "Groups: [");
        for (String group : message.groups) {
            LOG.log(Level.FINE, group);
        }
        LOG.log(Level.FINE, "]");
        message.status = ZreUtil.getNumber1(buffer);
        LOG.log(Level.FINE, "Status = {0}", message.status);
        message.name = ZreUtil.getString(buffer);
        LOG.log(Level.FINE, "Name = {0}", message.name);
        message.headers = ZreUtil.getHeaders(buffer);
        message.headers.entrySet().forEach(entry -> {
            String key = entry.getKey();
            String value = entry.getValue();
            LOG.log(Level.FINE, "key = {0} value = {1}", new Object[]{key, value});
        });
        return message;
    }
    
    public static void packHello(ByteArrayOutputStream baos, int sequence,
            String endpoint, String[] groups, int status, String name, 
            Map<String, String> headers) throws IOException {
        ZreUtil.setNumber2(baos, sequence);
        ZreUtil.setString(baos, endpoint);
        ZreUtil.setStrings(baos, groups);
        ZreUtil.setNumber1(baos, status);
        ZreUtil.setString(baos, name);
        ZreUtil.setHeaders(baos, headers);
    }

    public static ZreMessage unpackWhisper(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.WHISPER;
        message.sequence = ZreUtil.getNumber2(buffer);
        LOG.log(Level.FINE, "squence = {0}", message.sequence);
        message.msg = ZreUtil.getMessage(buffer);
        LOG.log(Level.FINE, "Message = [{0}]", new String(message.msg, ZMQ.CHARSET));
        return message;
    }

    public static void packWhisper(ByteArrayOutputStream baos, int sequence, byte[] msg) throws IOException {
        ZreUtil.setNumber2(baos, sequence);
        ZreUtil.setMessage(baos, msg);
    }
    
    public static ZreMessage unpackShout(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.SHOUT;
        message.sequence = ZreUtil.getNumber2(buffer);
        LOG.log(Level.FINE, "squence = {0}", message.sequence);
        message.group = ZreUtil.getString(buffer);
        LOG.log(Level.FINE, "Group = {0}", message.group);
        message.msg = ZreUtil.getMessage(buffer);
        LOG.log(Level.FINE, "Message = [{0}]", new String(message.msg, ZMQ.CHARSET));
        return message;
    }

    public static void packShout(ByteArrayOutputStream baos, int sequence,
            String group, byte[] msg) throws IOException {
        ZreUtil.setNumber2(baos, sequence);
        ZreUtil.setString(baos, group);
        ZreUtil.setMessage(baos, msg);
    }
    
    public static ZreMessage unpackJoin(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.JOIN;
        message.sequence = ZreUtil.getNumber2(buffer);
        LOG.log(Level.FINE, "squence = {0}", message.sequence);
        message.group = ZreUtil.getString(buffer);
        LOG.log(Level.FINE, "Group = {0}", message.group);
        message.status = ZreUtil.getNumber1(buffer);
        LOG.log(Level.FINE, "Status = {0}", message.status);
        return message;
    }
    
    public static void packJoin(ByteArrayOutputStream baos, int sequence,
            String group, int status) throws IOException {
        ZreUtil.setNumber2(baos, sequence);
        ZreUtil.setString(baos, group);
        ZreUtil.setNumber1(baos, status);
    }

    public static ZreMessage unpackLeave(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.LEAVE;
        message.sequence = ZreUtil.getNumber2(buffer);
        LOG.log(Level.FINE, "squence = {0}", message.sequence);
        message.group = ZreUtil.getString(buffer);
        LOG.log(Level.FINE, "Group = {0}", message.group);
        message.status = ZreUtil.getNumber1(buffer);
        LOG.log(Level.FINE, "Status = {0}", message.status);
        return message;
    }

    public static void packLeave(ByteArrayOutputStream baos, int sequence,
            String group, int status) throws IOException {
        ZreUtil.setNumber2(baos, sequence);
        ZreUtil.setString(baos, group);
        ZreUtil.setNumber1(baos, status);
    }
    
    public static ZreMessage unpackPing(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.PING;
        message.sequence = ZreUtil.getNumber2(buffer);
        LOG.log(Level.FINE, "squence = {0}", message.sequence);
        return message;
    }

    public static void packPing(ByteArrayOutputStream baos, int sequence) throws IOException {
        ZreUtil.setNumber2(baos, sequence);
    }
    
    public static ZreMessage unpackPingOk(ByteBuffer buffer) {
        ZreMessage message = new ZreMessage();
        message.eventType = ZreEventType.PING_OK;
        message.sequence = ZreUtil.getNumber2(buffer);
        LOG.log(Level.FINE, "squence = {0}", message.sequence);
        return message;
    }
    
    public static void packPingOk(ByteArrayOutputStream baos, int sequence) throws IOException {
        ZreUtil.setNumber2(baos, sequence);
    }

    public String getAddress() {
        return address;
    }

    public int getEventType() {
        return eventType;
    }

    public int getIdentity() {
        return identity;
    }

    public int getSequence() {
        return sequence;
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String[] getGroups() {
        return groups;
    }

    public String getGroup() {
        return group;
    }

    public int getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getMsg() {
        return msg;
    }
    
    @Override
    public String toString() {
        String string = "Event type = " + ZreEventType.toString(eventType);
        switch(eventType) {
            case ZreEventType.CONNECT:
                string += " identity = " + identity;
                string += " uuid = " +uuid.toString();
                break;
            case ZreEventType.HELLO:
                string += " sequence = " + sequence;
                string += " endpoint = " + endpoint;
                string += " groups = [" + String.join(",", groups) + "]";
                string += " status = " + status;
                string += " name = " + name;
                string += " headers nb elements = " + headers.size();
                break;
            case ZreEventType.PING:
                string += " sequence = " + sequence;
                break;
            case ZreEventType.PING_OK:
                string += " sequence = " + sequence;
                break;
            case ZreEventType.JOIN:
                string += " sequence = " + sequence;
                string += " group = " + group;
                string += " status = " + status;
                break;
            case ZreEventType.LEAVE:
                string += " sequence = " + sequence;
                string += " group = " + group;
                string += " status = " + status;
                break;
            case ZreEventType.SHOUT:
                string += " sequence = " + sequence;
                string += " group = " + group;
                string += " msg = " + new String(msg, ZMQ.CHARSET);
                break;
            case ZreEventType.WHISPER:
                string += " sequence = " + sequence;
                string += " msg = " + new String(msg, ZMQ.CHARSET);
                break;
        }
        
        
        return string;
    }
   
}
