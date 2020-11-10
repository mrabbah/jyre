package org.zeromq.jyre;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zeromq.Jyre;
import org.zeromq.Peer;
import org.zeromq.ZMQ;
import org.zeromq.ZreConstants;
import org.zeromq.ZreEventType;

/**
 *
 * @author rabbah
 */
public class Zpinger {

    public static void main(String[] args) {
        Jyre node = null;
        try {
            node = Jyre.getInstance(new Jyre.JyreListener() {
                @Override
                public void onMessage(Peer peer, int eventType, String group, byte[] msg, Jyre instance) {
                    /*System.out.println("receiving message from " + 
                            peer.name + " msg = " + 
                            new String(msg, ZMQ.CHARSET));*/
                    switch (eventType) {
                        case ZreEventType.HELLO:
                            //System.out.println("Receiving hello -> sending whisper");
                            try {
                                instance.whisper(peer.uuid.toString(), "Hello".getBytes(ZMQ.CHARSET));
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }

                            break;

                        case ZreEventType.WHISPER:
                            System.out.println(instance.getName() + " received ping (WHISPER)");
                            try {
                                instance.shout(ZreConstants.DEFAULT_GROUP, "Hello".getBytes(ZMQ.CHARSET));
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }

                            break;

                        case ZreEventType.SHOUT:
                            System.out.println(instance.getName() + " " + group + " received ping (SHOUT)");
                            break;
                    }
                }
            }, new HashMap<>());

            System.out.println("Node uuid = " + node.getUuid().toString()
                    + " name = " + node.getName());
            node.start();
            zmq.ZMQ.sleep(1, TimeUnit.HOURS);
            /*while (true) {
                zmq.ZMQ.sleep(1, TimeUnit.MINUTES);
            }*/
        } catch (IOException iOException) {
            iOException.printStackTrace();
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        } finally {
            if (node != null) {
                try {
                    node.disconnect();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }

    }
}
