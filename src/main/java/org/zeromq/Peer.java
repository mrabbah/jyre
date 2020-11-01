/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.zeromq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zeromq.ZMQ.Socket;

/**
 *
 * @author rabbah
 */
public class Peer {

    public UUID uuid; // Identity UUID
    public String name = "notset"; // Peer's public name
    public String endpoint; // Endpoint connected to
    private Jyre instance;
    public final Set<String> groups = new HashSet<>();
    public int status = 0; // Our status counter
    public int mailBoxPort;  // mail box port
    public final Map<String, String> headers = new HashMap<>(); // Peer headers
    public boolean connected = false; // Peer will send messages
    public boolean ready = false; // Peer has said Hello to us
    public int sentSequence = 0; // Outgoing message sequence
    public int wantSsequence = 0; // Incoming message sequence
    public long evasiveAt = 0; // Peer is being evasive
    public long expiredAt = 0; // Peer has expired by nows
    private Socket dealer;

    private static final Logger LOG = Logger.getLogger(Peer.class.getName());

    public Peer(InetAddress peerInetAddress, UUID peerUUID, int peerMailBoxPort,
            byte[] peerData, Jyre instance) {
        this.uuid = peerUUID;
        this.name = peerUUID.toString();
        this.mailBoxPort = peerMailBoxPort;
        String peerAddress = peerInetAddress.getHostAddress();
        this.endpoint = "tcp://" + peerAddress + ":" + peerMailBoxPort;
        this.instance = instance;
    }
    
    public Peer(String endpoint, UUID peerUUID, Jyre instance) {
        this.endpoint = endpoint;
        this.uuid = peerUUID;
        this.instance = instance;
        String[] endpoints = endpoint.split(":");
        this.mailBoxPort = Integer.valueOf(endpoints[2]);
        this.name = peerUUID.toString();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.disconnect();
    }

    public void refresh() {
        long now = System.currentTimeMillis();
        this.evasiveAt = ZreConstants.PEER_EVASIVE + now;
        this.expiredAt = ZreConstants.PEER_EXPIRED + now;
    }

    public void connect() throws IOException {
        
        if (!this.connected) {

            dealer = instance.context.createSocket(SocketType.DEALER);
            dealer.setSndHWM(
                    ZreConstants.NB_MESSAGE_TO_SEND_PER_SECONDS * 
                            ZreConstants.PEER_EXPIRED / 1000);
            dealer.setLinger(0);
            dealer.setImmediate(true);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ZreMessage.packConnect(baos, instance.uuid);
            dealer.setIdentity(baos.toByteArray());
            LOG.log(Level.INFO
                    , "connecting to peer {0} on {1}", new Object[]{uuid.toString(), endpoint});
            dealer.connect(this.endpoint);
            this.connected = true;
            this.ready = false;

        }

    }

    public void disconnect() {
        if (this.connected) {
            LOG.log(Level.INFO, "{0} Disconnecting peer {1}", 
                    new Object[]{instance.uuid.toString(), uuid.toString()});
            
            dealer.close();
            endpoint = "";
            connected = false;
            ready = false;
        }
    }

    private ByteArrayOutputStream initMessage(int eventType) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZreUtil.setNumber2(baos, 0xAAA1); // setting the signature
        ZreUtil.setNumber1(baos, eventType);
        ZreUtil.setNumber1(baos, ZreConstants.VERSION); // setting the version

        return baos;
    }

    private void sendMessage(ByteArrayOutputStream baos) {
        if (!this.connected) {
            LOG.log(Level.SEVERE, "Peer {0} is not connected", uuid.toString());
            return;
        }
        try {
            dealer.send(baos.toByteArray());
        } catch (ZMQException ex) {
            LOG.log(Level.SEVERE,
                    "Exception while sending message from  to {0} to {1} \nSequence = {2} \n Exception = {2}",
                    new Object[]{instance.uuid.toString(), uuid.toString(), 
                        this.sentSequence, ex.getMessage()});
            this.disconnect();
        }
    }

    public void sayHello() throws IOException {
        if(instance.address.equals("*")) {
            LOG.log(Level.SEVERE, "{0} cannot send hello to peer {1} address not yet set",
                    new Object[]{instance.uuid.toString(), uuid.toString()});
        } else {
            LOG.log(Level.INFO, "{0} sending hello to peer {1}", 
                new Object[]{instance.uuid.toString(), uuid.toString()});
            ByteArrayOutputStream baos = initMessage(ZreEventType.HELLO);
            
            ZreMessage.packHello(baos, ++sentSequence % 65535 , instance.getEndpoint(),
                    instance.groups.toArray(new String[instance.groups.size()]), 
                    ++status % 65535, instance.name, instance.headers);
            
            this.sendMessage(baos);
        }
        
    }

    public void whisper(byte[] msg) throws IOException {
        LOG.log(Level.INFO, "{0} whisper peer {1}", 
                new Object[]{instance.uuid.toString(), uuid.toString()});
        ByteArrayOutputStream baos = initMessage(ZreEventType.WHISPER);
        ZreMessage.packWhisper(baos, ++this.sentSequence % 65535, msg);
        this.sendMessage(baos);
    }

    public void shout(String group, byte[] msg) throws IOException {
        // if (groups.contains(group)) {
            LOG.log(Level.INFO, "{0} shout peer {1} since it belongs to {2}", 
                    new Object[]{instance.uuid.toString(), uuid.toString(), group});
            ByteArrayOutputStream baos = initMessage(ZreEventType.SHOUT);
            ZreMessage.packShout(baos, ++this.sentSequence % 65535, group, msg);
            this.sendMessage(baos);
        // }
    }

    public void join(String group) throws IOException {
        LOG.log(Level.INFO, "{0} sent join group {1} to peer {2}", 
                new Object[]{instance.uuid.toString(), group, uuid.toString()});
        ByteArrayOutputStream baos = initMessage(ZreEventType.JOIN);
        ZreMessage.packJoin(baos, ++this.sentSequence % 65535, group, ++status % 65535);
        this.sendMessage(baos);
    }

    public void Leave(String group) throws IOException {
        LOG.log(Level.INFO, "{0} sent leave group {1} to peer {2}", 
                new Object[]{instance.uuid.toString(), group, uuid.toString()});
        ByteArrayOutputStream baos = initMessage(ZreEventType.LEAVE);
        ZreMessage.packLeave(baos, ++sentSequence % 65535, group, ++status % 65535);
        this.sendMessage(baos);
    }

    public void Ping() throws IOException {
        LOG.log(Level.INFO, "{0} Ping peer {1}", 
                new Object[]{instance.uuid.toString(), uuid.toString()});
        ByteArrayOutputStream baos = initMessage(ZreEventType.PING);
        ZreMessage.packPing(baos, ++sentSequence % 65535);
        this.sendMessage(baos);
    }

    public void PingOk() throws IOException {
        LOG.log(Level.INFO, "{0} Ping OK peer {1}", 
                new Object[]{instance.uuid.toString(), uuid.toString()});
        ByteArrayOutputStream baos = initMessage(ZreEventType.PING_OK);
        ZreMessage.packPingOk(baos, ++sentSequence % 65535);
        this.sendMessage(baos);
    }
    
    public boolean messageLost(ZreMessage msg) {
        LOG.log(Level.INFO, "{0} recv {1} from peer={2} sequence={3}",
                new Object[]{instance.uuid.toString(), ZreEventType.toString(msg.getEventType()),
                uuid.toString(), msg.getSequence()});
        if(msg.getEventType() == ZreEventType.HELLO) {
            this.wantSsequence = 1;
        } else {
            this.wantSsequence++;
            this.wantSsequence = this.wantSsequence % 65535;
            
        }
        if(msg.getSequence() != this.wantSsequence) {
            LOG.log(Level.WARNING, "{0} seq error from peer={1} expect={2}, got={3}",
                new Object[]{instance.uuid.toString(), uuid.toString(),
                this.wantSsequence, msg.getSequence()});
            return true;
        }
        return false;
    }

}
