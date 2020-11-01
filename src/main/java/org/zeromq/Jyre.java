package org.zeromq;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZreBeacon.ZreBeaconStatus;
import org.zeromq.ZMQ.Socket;

public class Jyre implements ZreBeacon.Listener {

    private static final Logger LOG = Logger.getLogger(Jyre.class.getName());

    private ZreBeacon beacon;
    private final Map<String, Peer> peers = new HashMap<>();
    private final Map<String, Set<String>> peersGroups = new HashMap<>();
    private ZreRouter zreRouter;
    private static Jyre instance;

    protected UUID uuid;
    protected String name;
    protected String address;
    protected final Set<String> groups = new HashSet<>();
    protected final Map<String, String> headers = new HashMap<>();
    protected final AtomicInteger mailBoxPort = new AtomicInteger();
    protected final AtomicReference<Boolean> isRunning = new AtomicReference<>();
    protected ZContext context;
    protected final AtomicReference<Boolean> ignoreOwnMessage = new AtomicReference();

    private Jyre() {
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        disconnect();
    }

    public synchronized static Jyre getInstance() throws IOException {
        if (instance == null) {
            instance = new Jyre();
            instance.context = new ZContext();
            instance.groups.add(ZreConstants.DEFAULT_GROUP);
            instance.uuid = UUID.randomUUID();
            instance.mailBoxPort.set(Utils.findOpenPort());
            instance.name = instance.uuid.toString().toUpperCase().substring(0, 6);
            instance.isRunning.set(Boolean.FALSE);
            instance.ignoreOwnMessage.set(Boolean.FALSE);
            instance.address = "*";

            instance.initRouter(instance.mailBoxPort, instance.isRunning, instance.context);

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

    private void initRouter(AtomicInteger mailBoxPort,
            AtomicReference<Boolean> isRunning, ZContext context) {
        zreRouter = new ZreRouter(this,
                (ZreMessage msg) -> {
                    LOG.log(Level.INFO, msg.toString());

                    /*switch(msg.getEventType()) {
            case ZreEventType.CONNECT:
                Peer peer = peers.get(msg.getUuid().toString());
                if(peer != null) {
                    
                } else {
                    
                }
                break;
            case ZreEventType.HELLO:
                break;
            case ZreEventType.PING:
                break;
            case ZreEventType.PING_OK:
                break;
        }*/
                });
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
                Thread.sleep(1000);
            }
            if (beacon.getStatus() == ZreBeaconStatus.IDLE) {
                beacon.start();
            }
        }

    }

    public void stop() throws InterruptedException {
        if (isRunning.get()) {
            if (beacon.getStatus() == ZreBeaconStatus.RUNNING) {
                beacon.stop();
            }
            if (zreRouter.thread != null) {
                ignoreOwnMessage.set(Boolean.FALSE);
                zreRouter.thread.interrupt();
                zreRouter.thread.join();
            }

        }
    }

    public void disconnect() throws InterruptedException {
        this.mailBoxPort.set(0);
        Thread.sleep(ZreConstants.DEFAULT_BROADCAST_INTERVAL * 5);
        for (Map.Entry<String, Peer> entry : peers.entrySet()) {
            Peer peer = entry.getValue();
            peer.disconnect();
        }
        stop();
        context.close();
    }

    @Override
    public void onBeacon(InetAddress sender, UUID remoteUuid, int remoteMailBoxPort, byte[] data) {
        if (!ignoreOwnMessage.get() && remoteUuid.toString().equals(uuid.toString())) {
            address = sender.getHostAddress();
            ignoreOwnMessage.set(Boolean.TRUE);
            LOG.log(Level.INFO, "{0} endpoint updated = {1}",
                    new Object[]{uuid.toString(), getEndpoint()});
        } else {
            Peer peer = peers.get(remoteUuid.toString());
            if (peer != null) {
                String remoteEndpoint = "tcp://" + sender.getHostAddress() + ":" + remoteMailBoxPort;
                if (peer.endpoint.equals(remoteEndpoint)) {
                    peer.refresh();
                } else {
                    try {
                        peer.disconnect();
                        leaveMyGroups(peer);
                        peer = new Peer(sender, remoteUuid, remoteMailBoxPort, data, this);
                        peer.connect();
                        peer.refresh();
                        peer.sayHello();
                    } catch (IOException ex) {
                        LOG.log(Level.SEVERE, null, ex);
                    }
                }
            } else {
                LOG.log(Level.INFO, "------> New peer contact us {0}", remoteUuid.toString());
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

    private void leaveAllGroups(Peer peer) {
        for (Map.Entry<String, Set<String>> entry : peersGroups.entrySet()) {
            Set<String> peersUUID = entry.getValue();
            peersUUID.remove(peer.uuid.toString());

        }
    }

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
        private final RouterListener listner;
        private final Jyre instance;
        private Socket router;
        private Thread[] threads;
        // private Poller poller;

        private final Logger LOG = Logger.getLogger(ZreRouter.class.getName());

        public ZreRouter(Jyre instance,
                RouterListener listner) {
            this.instance = instance;
            this.instance.mailBoxPort.set(-1);
            this.listner = listner;
        }

        @Override
        public void run() {

            try {
                router = instance.context.createSocket(SocketType.ROUTER);
                instance.mailBoxPort.set(router.bindToRandomPort("tcp://" + ZreConstants.DEFAULT_LISTENING_INTERFACE));
                router.setRouterHandover(true);
                instance.isRunning.set(Boolean.TRUE);
                LOG.log(Level.INFO, "{0} current endpoint = {1}",
                        new Object[]{instance.uuid.toString(), instance.getEndpoint()});

                //  Backend socket talks to workers over inproc
                Socket backend = instance.context.createSocket(SocketType.DEALER);
                backend.bind("inproc://" + instance.name);

                threads = new Thread[5];
                //  Launch pool of worker threads, precise number is not critical
                for (int threadNbr = 0; threadNbr < 5; threadNbr++) {
                    thread = new Thread(new Worker(instance, this.listner));
                    thread.start();
                }

                //  Connect backend to frontend via a proxy
                ZMQ.proxy(router, backend, null);

            } finally {
                instance.isRunning.set(Boolean.FALSE);
                // poller.unregister(router);
                /*for (int threadNbr = 0; threadNbr < 5; threadNbr++) {
                    try {
                        thread.interrupt();
                        thread.join();
                    } catch (InterruptedException ex) {
                        LOG.log(Level.SEVERE, null, ex);
                    }
                }*/
                router.close();
                thread = null;
            }

        }

    }

    private class Worker implements Runnable {

        private final Jyre instance;
        private final Logger LOG = Logger.getLogger(Worker.class.getName());
        private final RouterListener listner;
        Socket worker;

        public Worker(Jyre instance, RouterListener listner) {
            this.instance = instance;
            this.listner = listner;
        }

        @Override
        public void run() {
            try {
                worker = instance.context.createSocket(SocketType.DEALER);
                worker.connect("inproc://" + instance.name);
                ByteBuffer identityBuffer;
                ByteBuffer contentBuffer;
                while (!Thread.currentThread().isInterrupted() && instance.isRunning.get()) {
                    //  The DEALER socket gives us the address envelope and message
                    ZMsg msg = ZMsg.recvMsg(worker);
                    ZFrame address = msg.pop();
                    ZFrame content = msg.pop();
                    assert (content != null);
                    msg.destroy();

                    LOG.log(Level.INFO, "address lenght = {0}", address.getData().length);
                    LOG.log(Level.INFO, "content lenght = {0}", content.getData().length);

                    identityBuffer = ByteBuffer.wrap(address.getData());
                    contentBuffer = ByteBuffer.wrap(content.getData());

                    ZreMessage identityMessage = ZreMessage.unpackConnect(identityBuffer);
                    this.listner.onMessage(identityMessage);

                    Peer peer = instance.peers.get(identityMessage.getUuid());

                    ZreMessage contentMessage = null;
                    int signature = ZreUtil.getNumber2(contentBuffer);

                    if (signature != 0xAAA1) {
                        continue;
                    }
                    int id = ZreUtil.getNumber1(contentBuffer);
                    LOG.log(Level.FINE, "Message type (ID) = {0}", id);
                    int version = ZreUtil.getNumber1(contentBuffer);
                    LOG.log(Level.FINE, "Version = {0}", version);
                    if (version != ZreConstants.VERSION) {
                        //TODO Throw Exception
                        continue;
                    }
                    if (id != ZreEventType.HELLO && (peer == null || peer.ready)) {
                        LOG.log(Level.SEVERE, "Invalid sequence from peer {0}",
                                identityMessage.getUuid().toString());
                        continue;
                    }
                    switch (id) {
                        case ZreEventType.HELLO: //Greeting
                            contentMessage = ZreMessage.unpackHello(contentBuffer);
                            if (peer == null) {
                                peer = new Peer(contentMessage.getEndpoint(),
                                        identityMessage.getUuid(), instance);
                                peer.groups.addAll(Arrays.asList(contentMessage.getGroups()));
                                instance.leaveAllGroups(peer);
                                instance.joinMyGroups(peer);
                                peer.name = contentMessage.getName();

                                if (contentMessage.getHeaders() != null) {
                                    peer.headers.putAll(contentMessage.getHeaders());
                                }

                                instance.peers.put(peer.uuid.toString(), peer);

                                try {
                                    peer.connect();
                                    peer.refresh();
                                    peer.sayHello();
                                    peer.messageLost(contentMessage);
                                    peer.ready = true;
                                    LOG.log(Level.INFO, "{0} peer entered", peer.name);

                                } catch (IOException ex) {
                                    LOG.log(Level.SEVERE, null, ex);
                                }

                            } else {
                                peer.messageLost(contentMessage);
                                peer.groups.clear();
                                peer.groups.addAll(Arrays.asList(contentMessage.getGroups()));
                                instance.leaveAllGroups(peer);
                                instance.joinMyGroups(peer);
                                peer.name = contentMessage.getName();

                                if (contentMessage.getHeaders() != null) {
                                    peer.headers.clear();
                                    peer.headers.putAll(contentMessage.getHeaders());
                                }
                                peer.ready = true;
                                LOG.log(Level.INFO, "{0} peer entered", peer.name);
                            }
                            break;
                        case ZreEventType.WHISPER:
                            contentMessage = ZreMessage.unpackWhisper(contentBuffer);
                            peer.messageLost(contentMessage);
                            this.listner.onMessage(contentMessage);
                            break;
                        case ZreEventType.SHOUT:
                            contentMessage = ZreMessage.unpackShout(contentBuffer);
                            peer.messageLost(contentMessage);
                            this.listner.onMessage(contentMessage);
                            break;
                        case ZreEventType.JOIN:
                            contentMessage = ZreMessage.unpackJoin(contentBuffer);
                            peer.messageLost(contentMessage);
                            peer.groups.add(contentMessage.getGroup());
                            Set<String> team = instance.peersGroups.get(contentMessage.getGroup());
                            if (team == null) {
                                team = new HashSet<>();
                                instance.peersGroups.put(contentMessage.getGroup(), team);
                            }
                            team.add(peer.uuid.toString());
                            LOG.log(Level.INFO, "Peer {0} joined group {1}",
                                    new Object[]{peer.name, contentMessage.getGroup()});
                            break;
                        case ZreEventType.LEAVE:
                            contentMessage = ZreMessage.unpackLeave(contentBuffer);
                            peer.messageLost(contentMessage);
                            peer.groups.remove(contentMessage.getGroup());
                            Set<String> myTeam = instance.peersGroups.get(contentMessage.getGroup());
                            if (myTeam != null) {
                                myTeam.remove(peer.uuid.toString());
                            }
                            LOG.log(Level.INFO, "Peer {0} leaved group {1}",
                                    new Object[]{peer.name, contentMessage.getGroup()});
                            break;
                        case ZreEventType.PING:
                            contentMessage = ZreMessage.unpackPing(contentBuffer);
                            peer.messageLost(contentMessage);
                            LOG.log(Level.INFO, "Receive ping from {0}", peer.name);

                            try {
                                peer.PingOk();
                            } catch (IOException ex) {
                                Logger.getLogger(Jyre.class.getName()).log(Level.SEVERE, null, ex);
                            }

                            break;

                        case ZreEventType.PING_OK:
                            contentMessage = ZreMessage.unpackPingOk(contentBuffer);
                            peer.messageLost(contentMessage);
                            LOG.log(Level.INFO, "Receive ping ok from {0}", peer.name);
                            break;
                        default:
                            byte[] data = ZreUtil.getMessage(contentBuffer);
                            LOG.log(Level.INFO, "remaining = {0}", new String(data, ZMQ.CHARSET));
                            break;
                        //TODO Throw Exception
                    }
                    /*if (contentMessage != null) {
                        this.listner.onMessage(contentMessage);
                    }*/
                    identityBuffer.clear();
                    contentBuffer.clear();
                    address.send(worker, ZFrame.REUSE + ZFrame.MORE);
                    content.send(worker, ZFrame.REUSE);
                    address.destroy();
                    content.destroy();
                }
            } /*catch (ZMQException e) {
                LOG.log(Level.SEVERE, null, e);
            }*/ finally {
                worker.close();
            }
        }

    }

    /**
     * All messages received in router are passed to a listener.
     */
    private interface RouterListener {

        void onMessage(ZreMessage message);
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
}
