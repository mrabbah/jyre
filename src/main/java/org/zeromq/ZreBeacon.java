package org.zeromq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import zmq.util.Objects;

/**
 * This class implements a peer-to-peer discovery service for local networks. A
 * beacon can broadcast and/or capture service announcements using UDP messages
 * on the local area network. This implementation uses IPv4 UDP broadcasts. You
 * can define the format of your outgoing beacons, and set a filter that
 * validates incoming beacons. Beacons are sent and received asynchronously in
 * the background.
 *
 */
public class ZreBeacon {

    enum ZreBeaconStatus {
        IDLE,
        RUNNING,
    }

    public static final long DEFAULT_BROADCAST_INTERVAL = 1000L;
    public static final String DEFAULT_BROADCAST_HOST = "255.255.255.255"; // is this the source/interface address? or the broadcast address
    private static final InetAddress DEFAULT_BROADCAST_HOST_ADDRESS;
    private static final InetAddress DEFAULT_BROADCAST_ADDRESS;
    private static final int DEFAULT_UDP_PORT = 5670;
    private static final byte[] DEFAULT_PREFIX = new byte[]{'Z', 'R', 'E', 0x01};

    static {
        try {
            DEFAULT_BROADCAST_HOST_ADDRESS = InetAddress.getByName(DEFAULT_BROADCAST_HOST);
            DEFAULT_BROADCAST_ADDRESS = InetAddress.getByName("0.0.0.0");
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid default broadcast address", e);
        }
    }

    private final AtomicReference<UUID> uuid = new AtomicReference<>();
    private final AtomicInteger mailBoxPort = new AtomicInteger();
    private final BroadcastClient broadcastClient;
    private final BroadcastServer broadcastServer;
    private final AtomicReference<byte[]> prefix = new AtomicReference<>();
    private final AtomicLong broadcastInterval = new AtomicLong(DEFAULT_BROADCAST_INTERVAL);
    private final AtomicReference<Listener> listener = new AtomicReference<>();
    private final AtomicReference<Thread.UncaughtExceptionHandler> clientExHandler = new AtomicReference<>();
    private final AtomicReference<Thread.UncaughtExceptionHandler> serverExHandler = new AtomicReference<>();
    private ZreBeaconStatus status;

    public ZreBeacon(UUID uuid, InetAddress broadcastAddress, InetAddress serverAddress,
            int port, long broadcastInterval, boolean ignoreLocalAddress,
             boolean ignoreOwnMessage, int mailBoxPort, byte[] prefix) {
        Objects.requireNonNull(broadcastAddress, "Host cannot be null");
        Objects.requireNonNull(serverAddress, "Server address cannot be null");
        Objects.requireNonNull(uuid, "UUID cannot be null");
        Objects.requireNonNull(mailBoxPort, "Mail box tcp port cannot be null");
        Objects.requireNonNull(prefix, "Prefix cannot be null");
        this.status = ZreBeaconStatus.IDLE;
        this.broadcastInterval.set(broadcastInterval);
        this.uuid.set(uuid);
        this.mailBoxPort.set(mailBoxPort);
        this.prefix.set(prefix);
        broadcastServer = new BroadcastServer(port, ignoreLocalAddress, ignoreOwnMessage);
        broadcastClient = new BroadcastClient(serverAddress, broadcastAddress, port, this.broadcastInterval);
    }

    public void setMailBoxPort(int mailBoxPort) {
        this.mailBoxPort.set(mailBoxPort);
    }

    /**
     *
     * @param listener
     */
    public void setListener(Listener listener) {
        this.listener.set(listener);
    }

    public static class Builder {

        private InetAddress broadcastHost = DEFAULT_BROADCAST_HOST_ADDRESS;
        private InetAddress interfaceAddr = DEFAULT_BROADCAST_ADDRESS;
        private int port = DEFAULT_UDP_PORT;
        private long broadcastInterval = DEFAULT_BROADCAST_INTERVAL;
        private byte[] prefix = DEFAULT_PREFIX;
        private boolean ignoreLocalAddress = false;
        private boolean ignoreOwnMessage = true;
        private UUID uuid;
        private int mailBoxPort;
        private Listener listener = null;

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder broadcastHost(InetAddress host) {
            this.broadcastHost = host;
            return this;
        }

        public Builder interfaceAddr(InetAddress addr) {
            this.interfaceAddr = addr;
            return this;
        }

        public Builder ignoreLocalAddress(boolean ignoreLocalAddress) {
            this.ignoreLocalAddress = ignoreLocalAddress;
            return this;
        }

        public Builder ignoreOwnMessage(boolean ignoreOwnMessage) {
            this.ignoreOwnMessage = ignoreOwnMessage;
            return this;
        }

        public Builder uuid(UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder mailBoxPort(int mailBoxPort) {
            this.mailBoxPort = mailBoxPort;
            return this;
        }

        public Builder broadcastInterval(long broadcastInterval) {
            this.broadcastInterval = broadcastInterval;
            return this;
        }

        public Builder prefix(byte[] prefix) {
            this.prefix = Arrays.copyOf(prefix, prefix.length);
            return this;
        }

        public Builder listener(Listener listener) {
            this.listener = listener;
            return this;
        }

        public ZreBeacon build() {

            ZreBeacon zbeacon = new ZreBeacon(uuid, broadcastHost, interfaceAddr,
                    port, broadcastInterval, ignoreLocalAddress, ignoreOwnMessage, mailBoxPort, prefix);

            if (listener != null) {
                zbeacon.setListener(listener);
            }
            return zbeacon;
        }
    }

    public void startClient() {
        if (!broadcastClient.isRunning) {
            if (broadcastClient.thread == null) {
                broadcastClient.thread = new Thread(broadcastClient);
                broadcastClient.thread.setName("ZreBeacon Client Thread " + uuid.get().toString());
                broadcastClient.thread.setDaemon(true);
                broadcastClient.thread.setUncaughtExceptionHandler(clientExHandler.get());
            }
            broadcastClient.thread.start();
        }
    }

    public void startServer() {
        if (!broadcastServer.isRunning) {
            if (listener.get() != null) {
                if (broadcastServer.thread == null) {
                    broadcastServer.thread = new Thread(broadcastServer);
                    broadcastServer.thread.setName("ZreBeacon Server Thread " + uuid.get().toString());
                    broadcastServer.thread.setDaemon(true);
                    broadcastServer.thread.setUncaughtExceptionHandler(serverExHandler.get());
                }
                broadcastServer.thread.start();
            }
        }
    }

    public void start() {
        startClient();
        startServer();
        this.status = ZreBeaconStatus.RUNNING;
    }

    public void stop() throws InterruptedException {
        if (broadcastClient.thread != null) {
            broadcastClient.thread.interrupt();
            broadcastClient.thread.join();
        }
        if (broadcastServer.thread != null) {
            broadcastServer.thread.interrupt();
            broadcastServer.thread.join();
        }
        this.status = ZreBeaconStatus.IDLE;
    }

    public ZreBeaconStatus getStatus() {
        return this.status;
    }

    /**
     * All beacons with matching prefix are passed to a listener.
     */
    public interface Listener {

        void onBeacon(InetAddress sender, UUID remoteUuid, int remoteMailBoxPort, byte[] data);
    }

    /**
     * The broadcast client periodically sends beacons via UDP to the network.
     */
    private class BroadcastClient implements Runnable {

        private final InetSocketAddress broadcastAddress;
        private final InetAddress interfaceAddress;
        private final AtomicLong broadcastInterval;
        private boolean isRunning;
        private Thread thread;

        public BroadcastClient(InetAddress interfaceAddress, InetAddress broadcastAddress, int port, AtomicLong broadcastInterval) {
            this.broadcastInterval = broadcastInterval;
            this.broadcastAddress = new InetSocketAddress(broadcastAddress, port);
            this.interfaceAddress = interfaceAddress;
        }

        @Override
        public void run() {
            try (DatagramChannel broadcastChannel = DatagramChannel.open()) {
                broadcastChannel.socket().setBroadcast(true);
                broadcastChannel.socket().setReuseAddress(true);
                broadcastChannel.socket().bind(new InetSocketAddress(interfaceAddress, 0));
                broadcastChannel.connect(broadcastAddress);

                isRunning = true;

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write(ZreBeacon.this.prefix.get());

                ByteBuffer uuidBuffer = ByteBuffer.wrap(new byte[16]);
                UUID uuid = ZreBeacon.this.uuid.get();
                uuidBuffer.putLong(uuid.getMostSignificantBits());
                uuidBuffer.putLong(uuid.getLeastSignificantBits());
                baos.write(uuidBuffer.array());

                byte[] tcpMailBoxBytePort = new byte[2];
                int tcpPort = ZreBeacon.this.mailBoxPort.get();

                byte[] myBytes = ByteBuffer.allocate(4).putInt(tcpPort).array();

                tcpMailBoxBytePort[0] = myBytes[2];
                tcpMailBoxBytePort[1] = myBytes[3];
                baos.write(tcpMailBoxBytePort);

                byte[] beacon = baos.toByteArray();

                while (!Thread.interrupted() && isRunning) {
                    try {
                        broadcastChannel.send(ByteBuffer.wrap(beacon), broadcastAddress);
                        Thread.sleep(broadcastInterval.get());
                    } catch (InterruptedException | ClosedByInterruptException interruptedException) {
                        // Re-interrupt the thread so the caller can handle it.
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception exception) {
                        throw new RuntimeException(exception);
                    }
                }
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            } finally {
                isRunning = false;
                thread = null;
            }
        }

    }

    /**
     * The broadcast server receives beacons.
     */
    private class BroadcastServer implements Runnable {

        private final DatagramChannel handle;            // Socket for send/recv
        private final boolean ignoreLocalAddress;
        private final boolean ignoreOwnMessage;
        private Thread thread;
        private boolean isRunning;

        public BroadcastServer(int port, boolean ignoreLocalAddress, boolean ignoreOwnMessage) {
            this.ignoreLocalAddress = ignoreLocalAddress;
            this.ignoreOwnMessage = ignoreOwnMessage;

            try {
                // Create UDP socket
                handle = DatagramChannel.open();
                handle.configureBlocking(true);
                handle.socket().setReuseAddress(true);
                handle.socket().bind(new InetSocketAddress(port));
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        @Override
        public void run() {
            ByteBuffer buffer = ByteBuffer.allocate(65535);
            SocketAddress sender;
            isRunning = true;
            byte[] prefix = ZreBeacon.this.prefix.get();
            int minAllowedPacketSize = prefix.length + 16 + 2;
            try {
                while (!Thread.interrupted() && isRunning) {
                    buffer.clear();
                    try {
                        sender = handle.receive(buffer);
                        InetAddress senderAddress = ((InetSocketAddress) sender).getAddress();

                        int bufferSize = buffer.position();

                        if (bufferSize < minAllowedPacketSize) { // bad packet format
                            continue;
                        }

                        if (ignoreLocalAddress
                                && (InetAddress.getLocalHost().getHostAddress().equals(senderAddress.getHostAddress())
                                || senderAddress.isAnyLocalAddress() || senderAddress.isLoopbackAddress())) {
                            continue;
                        }

                        buffer.flip();
                        buffer.mark();

                        byte[] prefixTry = new byte[prefix.length];
                        buffer.get(prefixTry);
                        if (Arrays.equals(prefix, prefixTry)) {
                            byte[] remoteByteUUid = new byte[16];

                            buffer.get(remoteByteUUid);
                            ByteBuffer bb = ByteBuffer.wrap(remoteByteUUid);
                            long high = bb.getLong();
                            long low = bb.getLong();
                            UUID remoteUuid = new UUID(high, low);

                            if (ignoreOwnMessage
                                    && (ZreBeacon.this.uuid.get().compareTo(remoteUuid) == 0)) {

                                continue;
                            }
                            byte[] remoteBytePort = new byte[2];

                            buffer.get(remoteBytePort);

                            byte[] remoteByteMailBoxPort
                                    = new byte[]{0, 0, remoteBytePort[0], remoteBytePort[1]};
                            ByteBuffer wrapped = ByteBuffer.wrap(remoteByteMailBoxPort);
                            int remoteMailBoxPort = wrapped.getInt();

                            byte[] data = new byte[buffer.remaining()];
                            buffer.get(data);

                            listener.get().onBeacon(senderAddress, remoteUuid, remoteMailBoxPort, data);
                        }

                    } catch (ClosedChannelException ioException) {
                        break;
                    } catch (IOException ioException) {
                        isRunning = false;
                        throw new RuntimeException(ioException);
                    }
                }
            } finally {
                handle.socket().close();
                isRunning = false;
                thread = null;
            }
        }

    }

    public long getBroadcastInterval() {
        return broadcastInterval.get();
    }

    public void setBroadcastInterval(long broadcastInterval) {
        this.broadcastInterval.set(broadcastInterval);
    }
}
