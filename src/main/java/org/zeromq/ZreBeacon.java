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
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final InetAddress DEFAULT_BROADCAST_HOST_ADDRESS;
    private static final InetAddress DEFAULT_BROADCAST_ADDRESS;

    static {
        try {
            DEFAULT_BROADCAST_HOST_ADDRESS = InetAddress.getByName(ZreConstants.DEFAULT_BROADCAST_HOST);
            DEFAULT_BROADCAST_ADDRESS = InetAddress.getByName("0.0.0.0");
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid default broadcast address", e);
        }
    }

    private final AtomicReference<UUID> uuid = new AtomicReference<>();
    private AtomicInteger mailBoxPort = new AtomicInteger();
    private final BroadcastClient broadcastClient;
    private final BroadcastServer broadcastServer;
    private final AtomicReference<Listener> listener = new AtomicReference<>();
    private final AtomicReference<Thread.UncaughtExceptionHandler> clientExHandler = new AtomicReference<>();
    private final AtomicReference<Thread.UncaughtExceptionHandler> serverExHandler = new AtomicReference<>();
    private ZreBeaconStatus status;

    public ZreBeacon(UUID uuid, InetAddress broadcastAddress, InetAddress serverAddress,
            int port, boolean ignoreLocalAddress,
            AtomicReference<Boolean> ignoreOwnMessage, AtomicInteger mailBoxPort) {
        Objects.requireNonNull(broadcastAddress, "Host cannot be null");
        Objects.requireNonNull(serverAddress, "Server address cannot be null");
        Objects.requireNonNull(uuid, "UUID cannot be null");
        Objects.requireNonNull(mailBoxPort, "Mail box tcp port cannot be null");
        Objects.requireNonNull(ignoreOwnMessage, "Ignore Own message cannot be null");
        this.status = ZreBeaconStatus.IDLE;
        this.uuid.set(uuid);
        this.mailBoxPort = mailBoxPort;
        broadcastServer = new BroadcastServer(port, ignoreLocalAddress, ignoreOwnMessage);
        broadcastClient = new BroadcastClient(serverAddress, broadcastAddress, port);
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
        private int port = ZreConstants.DEFAULT_UDP_PORT;
        private boolean ignoreLocalAddress = false;
        private AtomicReference<Boolean> ignoreOwnMessage;
        private UUID uuid;
        private AtomicInteger mailBoxPort;
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

        public Builder ignoreOwnMessage(AtomicReference<Boolean> ignoreOwnMessage) {
            this.ignoreOwnMessage = ignoreOwnMessage;
            return this;
        }

        public Builder uuid(UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder mailBoxPort(AtomicInteger mailBoxPort) {
            this.mailBoxPort = mailBoxPort;
            return this;
        }

        public Builder listener(Listener listener) {
            this.listener = listener;
            return this;
        }

        public ZreBeacon build() {

            ZreBeacon zbeacon = new ZreBeacon(uuid, broadcastHost, interfaceAddr,
                    port, ignoreLocalAddress, ignoreOwnMessage,
                    mailBoxPort);

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
    protected interface Listener {

        void onBeacon(InetAddress sender, UUID remoteUuid, int remoteMailBoxPort, byte[] data);
    }

    /**
     * The broadcast client periodically sends beacons via UDP to the network.
     */
    private class BroadcastClient implements Runnable {

        private final InetSocketAddress broadcastAddress;
        private final InetAddress interfaceAddress;
        private boolean isRunning;
        private Thread thread;
        private final Logger LOG = Logger.getLogger(BroadcastClient.class.getName());

        public BroadcastClient(InetAddress interfaceAddress, InetAddress broadcastAddress, int port) {
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
                ZreUtil.setPrefix(baos);

                ZreUtil.setUUID(baos, ZreBeacon.this.uuid.get());

                int tcpPort = ZreBeacon.this.mailBoxPort.get();

                ZreUtil.setNumber2(baos, tcpPort);

                byte[] beacon = baos.toByteArray();

                while (!Thread.interrupted() && isRunning) {
                    try {
                        broadcastChannel.send(ByteBuffer.wrap(beacon), broadcastAddress);
                        Thread.sleep(ZreConstants.DEFAULT_BROADCAST_INTERVAL);
                    } catch (InterruptedException | ClosedByInterruptException interruptedException) {
                        // Re-interrupt the thread so the caller can handle it.
                        Thread.currentThread().interrupt();
                        break;
                    } catch (IOException ioException) {
                        LOG.log(Level.SEVERE, "Interface changed", ioException);
                        throw new RuntimeException(ioException);
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
        private final AtomicReference<Boolean> ignoreOwnMessage;
        private Thread thread;
        private boolean isRunning;

        public BroadcastServer(int port, boolean ignoreLocalAddress, AtomicReference<Boolean> ignoreOwnMessage) {
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
            int minAllowedPacketSize = ZreConstants.DEFAULT_PREFIX.length + 16 + 2;
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

                        byte[] prefixTry = ZreUtil.getPrefix(buffer);
                        if (Arrays.equals(ZreConstants.DEFAULT_PREFIX, prefixTry)) {

                            UUID remoteUuid = ZreUtil.getUUID(buffer);

                            if (ignoreOwnMessage.get()
                                    && (ZreBeacon.this.uuid.get().compareTo(remoteUuid) == 0)) {

                                continue;
                            }

                            int remoteMailBoxPort = ZreUtil.getNumber2(buffer);

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

}
