package org.zeromq;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zeromq.ZreBeacon.ZreBeaconStatus;
import org.zeromq.ZMQ.Socket;

public class Jyre implements ZreBeacon.Listener {
    
    private static final Logger LOG = Logger.getLogger(Jyre.class.getName());
    
    private ZreBeacon beacon;
    private final Map<String, Peer> peers = new HashMap<>(); // List of all my peers
    private final Map<String, Set<String>> peersGroups = new HashMap<>(); // List of groups and for each groupe peers uuid that belong to this group
    private ZreRouter zreRouter;
    private Heartbeating heartbeating;
    private static Jyre instance;
    
    protected UUID uuid;
    protected String name;
    protected String address;
    protected final Set<String> groups = new HashSet<>();
    protected final Map<String, String> headers;
    protected final AtomicInteger mailBoxPort = new AtomicInteger();
    protected final AtomicReference<Boolean> isRunning = new AtomicReference<>();
    protected ZContext context;
    protected final AtomicReference<Boolean> ignoreOwnMessage = new AtomicReference();
    private final JyreListener listner;
    
    private Jyre(JyreListener listner, Map<String, String> headers) {
        this.listner = listner;
        this.headers = headers;
    }
    
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        disconnect();
    }
    
    public synchronized static Jyre getInstance(JyreListener listner, Map<String, String> headers) throws IOException {
        if (instance == null) {
            instance = new Jyre(listner, headers);
            instance.context = new ZContext();
            instance.groups.add(ZreConstants.DEFAULT_GROUP);
            instance.uuid = UUID.randomUUID();
            instance.mailBoxPort.set(Utils.findOpenPort());
            instance.name = instance.uuid.toString().toUpperCase().substring(0, 6);
            instance.isRunning.set(Boolean.FALSE);
            instance.ignoreOwnMessage.set(Boolean.FALSE);
            instance.address = "*";
            
            instance.initRouter();
            
            ZreBeacon.Builder builder = new ZreBeacon.Builder()
                    .uuid(instance.uuid)
                    .mailBoxPort(instance.mailBoxPort)
                    .ignoreLocalAddress(false)
                    .ignoreOwnMessage(instance.ignoreOwnMessage)
                    .listener(instance);
            instance.beacon = builder.build();
            
        }
        return instance;
    }
    
    private void initRouter() {
        zreRouter = new ZreRouter(this);
        heartbeating = new Heartbeating(this);
    }
    
    public void start() throws IOException, InterruptedException {
        if (!isRunning.get()) {
            //int newMailBoxPort = Utils.findOpenPort();
            // mailBoxPort.set(newMailBoxPort);
            if (zreRouter.thread == null) {
                zreRouter.thread = new Thread(zreRouter);
                zreRouter.thread.setName("Zre Router thread "
                        + uuid.toString());
                zreRouter.thread.setDaemon(true);
                zreRouter.thread.start();
            }
            if (beacon.getStatus() == ZreBeaconStatus.IDLE) {
                // LOG.info("Waiting DEALER and ROUTER start before broadcasting beacons");
                System.out.println("Waiting DEALER and ROUTER start before broadcasting beacons");
                while (!isRunning.get()) { // Waiting DEALER and ROUTER start before broadcasting beacons
                    System.out.print(".");
                    Thread.sleep(10);
                }
                System.out.println(".");
                // LOG.info("Starting broadcasting beacons");
                System.out.println("Starting broadcasting beacons");
                beacon.start();
            }
            if (heartbeating.thread == null) {
                heartbeating.thread = new Thread(heartbeating);
                heartbeating.thread.setName("Heartbeating thread "
                        + uuid.toString());
                heartbeating.thread.setDaemon(true);
                heartbeating.thread.start();
            }
        }
        
    }
    
    public void stop() throws InterruptedException {
        System.out.println("Stopping the node");
        if (isRunning.get()) {
            if (beacon.getStatus() == ZreBeaconStatus.RUNNING) {
                System.out.println("Stopping braodcasting Beacons");
                beacon.stop();
            }
            if (zreRouter.thread != null) {
                System.out.println("Stopping the Router");
                ignoreOwnMessage.set(Boolean.FALSE);
                zreRouter.thread.interrupt();
                zreRouter.thread.join();
            }
            
            if (heartbeating.thread != null) {
                System.out.println("Stopping heartbeating");
                heartbeating.thread.interrupt();
                heartbeating.thread.join();
            }
            
        }
    }
    
    public void disconnect() throws InterruptedException {
        System.out.println("Disconnecting from the Matrix...");
        this.mailBoxPort.set(0);
        System.out.println("Broadcasting the disconnect event on beacons");
        Thread.sleep(ZreConstants.DEFAULT_BROADCAST_INTERVAL * 5);
        System.out.println("Disconnecting peers");
        for (Map.Entry<String, Peer> entry : peers.entrySet()) {
            Peer peer = entry.getValue();
            System.out.println("Disconnecting peer " + peer.name);
            peer.disconnect();
        }
        stop();
        System.out.println("Closing ZMQ Context");
        context.close();
    }
    
    public boolean join(String group) throws IOException {
        if (!this.groups.contains(group)) {
            return false;
        }
        System.out.println("Joining group " + group);
        this.groups.add(group);
        for (Peer peer : peers.values()) {
            peer.join(group);
        }
        return true;
    }
    
    public boolean leave(String group) throws IOException {
        if (this.groups.contains(group)) {
            System.out.println("Leaving group " + group);
            this.groups.remove(group);
            for (Peer peer : peers.values()) {
                peer.Leave(group);
            }
        }
        return true;
    }
    
    public boolean whisper(String struuid, byte[] msg) throws IOException {
        Peer peer = peers.get(struuid);
        if(peer != null) {
            peer.whisper(msg);
            return true;
        }
        return false;
    }
    
    public boolean shout(String group, byte[] msg) throws IOException {
        Set<String> peersUUID = peersGroups.get(msg);
        if(peersUUID == null || peersUUID.isEmpty()) {
            return false;
        }
        for(String peerUUID: peersUUID) {
            Peer peer = peers.get(peerUUID);
            if(peer != null) {
                peer.shout(group, msg);
            } else {
                LOG.log(Level.SEVERE, "peer " + peerUUID + " not found in group " + group);
                peers.remove(peerUUID);
            }
        }
        return true;
    }
    
    @Override
    public void onBeacon(InetAddress sender, UUID remoteUuid, int remoteMailBoxPort, byte[] data) {
        if (!ignoreOwnMessage.get() && remoteUuid.toString().equals(uuid.toString())) {
            address = sender.getHostAddress();
            ignoreOwnMessage.set(Boolean.TRUE);
            /*LOG.log(Level.INFO, "{0} endpoint updated = {1}",
                    new Object[]{uuid.toString(), getEndpoint()});*/
            System.out.println(uuid.toString() + " endpoint updated = " + getEndpoint());
        } else {
            Peer peer = peers.get(remoteUuid.toString());
            if (peer != null) {
                String remoteEndpoint = "tcp://" + sender.getHostAddress() + ":" + remoteMailBoxPort;
                if (remoteMailBoxPort == 0) {
                    System.out.println("Peer " + remoteUuid.toString() + " are leaving the Matrix");
                    peer.disconnect();
                    leaveMyGroups(peer);
                    peers.remove(peer.uuid.toString());
                } else if (peer.getEndpoint().equals(remoteEndpoint)) {
                    peer.refresh();
                } else {
                    try {
                        peer.disconnect();
                        // leaveMyGroups(peer);
                        peer = new Peer(sender, remoteUuid, remoteMailBoxPort, data, this);
                        peer.connect();
                        peer.refresh();
                        peer.sayHello();
                    } catch (IOException ex) {
                        LOG.log(Level.SEVERE, null, ex);
                    }
                }
            } else {
                // LOG.log(Level.INFO, "------> New peer contact us {0}", remoteUuid.toString());
                System.out.println("UDP: New peer contact us " + remoteUuid.toString());
                try {
                    peer = new Peer(sender, remoteUuid, remoteMailBoxPort, data, this);
                    peers.put(remoteUuid.toString(), peer);
                    peer.connect();
                    peer.refresh();
                    peer.sayHello();
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            }
        }
        
    }
    
    private void leaveMyGroups(Peer peer) {
        for (String group : peer.groups) {
            Set<String> partners = peersGroups.get(group);
            if (partners != null) {
                partners.remove(peer.uuid.toString());
            }
        }
    }

    /*private void leaveAllGroups(Peer peer) {
        for (Map.Entry<String, Set<String>> entry : peersGroups.entrySet()) {
            Set<String> peersUUID = entry.getValue();
            peersUUID.remove(peer.uuid.toString());

        }
    }*/
    private void joinMyGroups(Peer peer) {
        for (String group : peer.groups) {
            Set<String> partners = peersGroups.get(group);
            if (partners == null) {
                partners = new HashSet<>();
                peersGroups.put(group, partners);
                
            }
            partners.add(peer.uuid.toString());
        }
    }
    
    private class ZreRouter implements Runnable {
        
        private Thread thread;
        private final Jyre instance;
        private Socket router;
        private final Thread[] threads;
        // private Poller poller;

        private final Logger LOG = Logger.getLogger(ZreRouter.class.getName());
        
        public ZreRouter(Jyre instance) {
            this.instance = instance;
            this.instance.mailBoxPort.set(-1);
            this.threads = new Thread[ZreConstants.DEFAULT_NB_WORKERS];
        }
        
        @Override
        public void run() {
            System.out.println("Runngin the ROUTER....");
            try {
                router = instance.context.createSocket(SocketType.ROUTER);
                instance.mailBoxPort.set(router.bindToRandomPort("tcp://" + ZreConstants.DEFAULT_LISTENING_INTERFACE));
                router.setRouterHandover(true);
                /*LOG.log(Level.INFO, "{0} current endpoint = {1}",
                        new Object[]{instance.uuid.toString(), instance.getEndpoint()});*/
                System.out.println(instance.uuid.toString() + " current ROUTER endpoint = " + instance.getEndpoint());

                //  Backend socket talks to workers over inproc
                System.out.println("Creating the dealer...");
                Socket backend = instance.context.createSocket(SocketType.DEALER);
                backend.bind("inproc://" + instance.name);

                //  Launch pool of worker threads, precise number is not critical
                System.out.println("Lunching pool of worker threads");
                for (int threadNbr = 0; threadNbr < ZreConstants.DEFAULT_NB_WORKERS; threadNbr++) {
                    threads[threadNbr] = new Thread(new Worker(instance, "Worker-" + threadNbr));
                    threads[threadNbr].start();
                }
                
                instance.isRunning.set(Boolean.TRUE);
                System.out.println("Node is running");
                //  Connect backend to frontend via a proxy
                System.out.println("Connect backend (Worker) to the frontend (Dealer) via a proxy");
                ZMQ.proxy(router, backend, null);
                
            } finally {
                System.out.println("Stoping the ROUTER...");
                instance.isRunning.set(Boolean.FALSE);
                // poller.unregister(router);
                System.out.println("Stoping the Workers...");
                for (int threadNbr = 0; threadNbr < ZreConstants.DEFAULT_NB_WORKERS; threadNbr++) {
                    try {
                        threads[threadNbr].interrupt();
                        threads[threadNbr].join();
                    } catch (InterruptedException ex) {
                        LOG.log(Level.SEVERE, null, ex);
                    }
                }
                System.out.println("Closing the ROUTER ....");
                router.close();
                thread = null;
            }
            
        }
        
    }
    
    private class Worker implements Runnable {
        
        private final Jyre instance;
        private final Logger LOG = Logger.getLogger(Worker.class.getName());
        private final String name;
        Socket worker;
        
        public Worker(Jyre instance, String name) {
            this.instance = instance;
            this.name = name;
        }
        
        @Override
        public void run() {
            System.out.println(this.name + " Start running...");
            try {
                System.out.println("Creating the DEALER SOCKET in to " + this.name);
                worker = instance.context.createSocket(SocketType.DEALER);
                worker.connect("inproc://" + instance.name);
                ByteBuffer identityBuffer;
                ByteBuffer contentBuffer;
                while (!Thread.currentThread().isInterrupted() && instance.isRunning.get()) {
                    //  The DEALER socket gives us the address envelope and message
                    System.out.println(this.name + " waiting to receive a message...");
                    ZMsg msg = ZMsg.recvMsg(worker);
                    System.out.println(this.name + " received a message...");
                    //System.out.println("msg content size = " + msg.contentSize());
                    ZFrame address = msg.pop();
                    // worker.getIdentity()
                    // LOG.log(Level.INFO, "address lenght = {0}", address.getData().length);
                    // System.out.println("address lenght = " + address.getData().length);
                    // LOG.log(Level.INFO, "content lenght = {0}", content.getData().length);
                    identityBuffer = ByteBuffer.wrap(address.getData());
                    // System.out.println("msg content size after pop = " + msg.contentSize());
                    contentBuffer = ByteBuffer.allocate((int) msg.contentSize());
                    ZFrame content = msg.pop();
                    contentBuffer.put(content.getData());
                    while (content.hasMore()) {
                        content = msg.pop();
                        contentBuffer.put(content.getData());
                    }
                    
                    msg.destroy();
                    contentBuffer.flip();

                    // contentBuffer = ByteBuffer.wrap(content.getData());
                    ZreMessage identityMessage = ZreMessage.unpackConnect(identityBuffer);
                    // this.listner.onMessage(identityMessage);
                    System.out.println("Peer Identity : " + identityMessage.toString());
                    
                    int signature = ZreUtil.getNumber2(contentBuffer);
                    
                    if (signature != 0xAAA1) {
                        LOG.log(Level.WARNING, "Peer {0} signature incorrect ",
                                identityMessage.getUuid().toString());
                        continue;
                    }
                    int id = ZreUtil.getNumber1(contentBuffer);
                    LOG.log(Level.FINE, "Message type (ID) = {0}", id);
                    int version = ZreUtil.getNumber1(contentBuffer);
                    LOG.log(Level.FINE, "Version = {0}", version);
                    if (version != ZreConstants.VERSION) {
                        LOG.log(Level.WARNING, "Peer {0} message version incorrect ",
                                identityMessage.getUuid().toString());
                        //TODO Throw Exception
                        continue;
                    }
                    
                    Peer peer = this.instance.peers.get(identityMessage.getUuid().toString());
                    
                    if (peer == null) {
                        System.out.println("Message are coming from unkowen peer " + identityMessage.getUuid().toString());
                        System.out.println("ROUTER CREATE NEW PEER...");
                        peer = new Peer(identityMessage.getUuid(), instance);
                        peers.put(identityMessage.getUuid().toString(), peer);
                    }
                    
                    peer.refresh();
                    /*if (id != ZreEventType.HELLO && (peer == null || !peer.ready)) {
                        LOG.log(Level.WARNING, "Invalid sequence from peer {0}",
                                identityMessage.getUuid().toString());
                        continue;
                    }*/
                    switch (id) {
                        case ZreEventType.HELLO: //Greeting
                            this.processHelloMessage(peer, identityMessage, contentBuffer);
                            break;
                        case ZreEventType.WHISPER:
                            if (peer.ready) {
                                this.processWhisperMessage(peer, contentBuffer);
                            }
                            break;
                        case ZreEventType.SHOUT:
                            if (peer.ready) {
                                this.processShoutMessage(peer, contentBuffer);
                            }
                            break;
                        case ZreEventType.JOIN:
                            if (peer.ready) {
                                this.processJoinMessage(peer, contentBuffer);
                            }
                            break;
                        case ZreEventType.LEAVE:
                            if (peer.ready) {
                                this.processLeaveMessage(peer, contentBuffer);
                            }
                            break;
                        case ZreEventType.PING:
                            if (peer.ready) {
                                this.processPingMessage(peer, contentBuffer);
                            }
                            break;
                        case ZreEventType.PING_OK:
                            if (peer.ready) {
                                this.processPingOkMessage(peer, contentBuffer);
                            }
                            break;
                        default:
                            byte[] data = ZreUtil.getMessage(contentBuffer);
                            LOG.log(Level.WARNING, "UNKNOWEN MESSAGE DATA TYPE {0}: data = {1}",
                                    new Object[]{id, new String(data, ZMQ.CHARSET)});
                            break;
                        //TODO Throw Exception
                    }
                    /*if (contentMessage != null) {
                        this.listner.onMessage(contentMessage);
                    }*/
                    System.out.println("cleaning address and content...");
                    identityBuffer.clear();
                    contentBuffer.clear();
                    address.send(worker, ZFrame.REUSE + ZFrame.MORE);
                    content.send(worker, ZFrame.REUSE);
                    address.destroy();
                    content.destroy();
                }
            } catch (ZMQException e) {
                LOG.log(Level.WARNING, e.toString());
            } finally {
                System.out.println("Closing " + this.name);
                worker.close();
            }
        }
        
        private void processHelloMessage(Peer peer, ZreMessage identityMessage, ByteBuffer contentBuffer) {
            System.out.println("Processing hello message from " + peer.uuid.toString());
            try {
                ZreMessage contentMessage = ZreMessage.unpackHello(contentBuffer);
                
                peer.checkMessageHasBeenLost(contentMessage);
                instance.leaveMyGroups(peer);
                peer.groups.clear();
                peer.groups.addAll(Arrays.asList(contentMessage.getGroups()));
                // instance.leaveAllGroups(peer);
                instance.joinMyGroups(peer);
                peer.name = contentMessage.getName();
                peer.setEndpoint(contentMessage.getEndpoint());
                peer.status = contentMessage.getStatus();
                
                if (contentMessage.getHeaders() != null) {
                    peer.headers.clear();
                    peer.headers.putAll(contentMessage.getHeaders());
                }
                if (!peer.connected) {
                    System.out.println("Receiving hello message from disconnected peer try to connect...");
                    peer.connect();
                    peer.sayHello();
                }
                peer.ready = true;
                // LOG.log(Level.INFO, "{0} peer entered", peer.name);
                System.out.println(peer.name + " peer entered");
            } catch (IOException iOException) {
                LOG.log(Level.SEVERE, iOException.getMessage());
            }
            
        }
        
        private void processWhisperMessage(Peer peer, ByteBuffer contentBuffer) {
            System.out.println("Processing whisper message from " + peer.uuid.toString());
            ZreMessage contentMessage = ZreMessage.unpackWhisper(contentBuffer);
            peer.checkMessageHasBeenLost(contentMessage);
            instance.listner.onMessage(peer, contentMessage.getMsg());
            
        }
        
        private void processShoutMessage(Peer peer, ByteBuffer contentBuffer) {
            System.out.println("Processing shout message from " + peer.uuid.toString());
            ZreMessage contentMessage = ZreMessage.unpackShout(contentBuffer);
            peer.checkMessageHasBeenLost(contentMessage);
            if (instance.groups.contains(contentMessage.getGroup())) {
                instance.listner.onMessage(peer, contentMessage.getMsg());
            }
        }
        
        private void processJoinMessage(Peer peer, ByteBuffer contentBuffer) {
            System.out.println("Processing join message from " + peer.uuid.toString());
            ZreMessage contentMessage = ZreMessage.unpackJoin(contentBuffer);
            peer.checkMessageHasBeenLost(contentMessage);
            peer.groups.add(contentMessage.getGroup());
            Set<String> team = instance.peersGroups.get(contentMessage.getGroup());
            if (team == null) {
                team = new HashSet<>();
                instance.peersGroups.put(contentMessage.getGroup(), team);
            }
            team.add(peer.uuid.toString());
            /*LOG.log(Level.INFO, "Peer {0} joined group {1}",
                    new Object[]{peer.name, contentMessage.getGroup()});*/
            System.out.println("Peer " + peer.name + " joined group " + contentMessage.getGroup());
        }
        
        private void processLeaveMessage(Peer peer, ByteBuffer contentBuffer) {
            System.out.println("Processing leave message from " + peer.uuid.toString());
            ZreMessage contentMessage = ZreMessage.unpackLeave(contentBuffer);
            peer.checkMessageHasBeenLost(contentMessage);
            peer.groups.remove(contentMessage.getGroup());
            Set<String> myTeam = instance.peersGroups.get(contentMessage.getGroup());
            if (myTeam != null) {
                myTeam.remove(peer.uuid.toString());
            }
            /*LOG.log(Level.INFO, "Peer {0} leaved group {1}",
                    new Object[]{peer.name, contentMessage.getGroup()});*/
            System.out.println("Peer " + peer.name + " leaved group " + contentMessage.getGroup());
        }
        
        private void processPingMessage(Peer peer, ByteBuffer contentBuffer) {
            System.out.println("Processing ping message from " + peer.uuid.toString());
            ZreMessage contentMessage = ZreMessage.unpackPing(contentBuffer);
            peer.checkMessageHasBeenLost(contentMessage);
            // LOG.log(Level.INFO, "Receive ping from {0}", peer.name);
            System.out.println("Receive ping from " + peer.name);
            
            try {
                peer.PingOk();
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, ex.getMessage());
            }
        }
        
        private void processPingOkMessage(Peer peer, ByteBuffer contentBuffer) {
            System.out.println("Processing PingOK message from " + peer.uuid.toString());
            ZreMessage contentMessage = ZreMessage.unpackPingOk(contentBuffer);
            peer.checkMessageHasBeenLost(contentMessage);
            // LOG.log(Level.INFO, "Receive ping ok from {0}", peer.name);
            System.out.println("Receive ping ok from " + peer.name);
        }
        
    }
    
    private class Heartbeating implements Runnable {
        
        private final Jyre instance;
        private Thread thread;
        private final Logger LOG = Logger.getLogger(Heartbeating.class.getName());
        
        public Heartbeating(Jyre instance) {
            this.instance = instance;
        }
        
        @Override
        public void run() {
            try {
                long now;
                while (true) {
                    now = System.currentTimeMillis();
                    Collection<Peer> peers = instance.peers.values();
                    for (Peer peer : peers) {
                        if (now > peer.expiredAt) {
                            System.out.println(peer.name + " peer exited");
                            System.out.println("Disconnecting peer " + peer.name);
                            peer.disconnect();
                            instance.leaveMyGroups(peer);
                            instance.peers.remove(peer.uuid.toString());
                        } else if (now > peer.evasiveAt) {
                            System.out.println(peer.name + " peer disapear from matrix try to ping it");
                            peer.Ping();
                        }
                    }
                    Thread.sleep(ZreConstants.REGULAR_CHECK_INTERVAL);
                }
            } catch (InterruptedException ex) {
                LOG.log(Level.SEVERE, ex.getMessage());
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, ex.getLocalizedMessage());
            } finally {
                this.thread = null;
            }
        }
        
    }

    /**
     * All messages received in router are passed to a listener.
     */
    public interface JyreListener {
        
        void onMessage(Peer peer, byte[] msg);
    }
    
    public UUID getUuid() {
        return uuid;
    }
    
    public String getName() {
        return name;
    }
    
    public String getEndpoint() {
        return "tcp://" + address + ":" + mailBoxPort.get();
    }

    public Map<String, Peer> getPeers() {
        return peers;
    }

    public Map<String, Set<String>> getPeersGroups() {
        return peersGroups;
    }

    public Set<String> getGroups() {
        return groups;
    }
    
    
}
