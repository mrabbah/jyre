package org.zeromq.jyre;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zeromq.Jyre;
import org.zeromq.Peer;
import org.zeromq.ZMQ;

/**
 * JYRE MAIN APP
 *
 */
public class App {

    public static void main(String[] args) {
        Jyre instance = null;
        try {
            setLevel(Level.INFO);
            System.out.println("JYRE Test!");
            instance = Jyre.getInstance(new Jyre.JyreListener() {
                @Override
                public void onMessage(Peer peer, byte[] msg) {
                    System.out.println("receiving message from " + 
                            peer.name + " msg = " + 
                            new String(msg, ZMQ.CHARSET));
                }
            }, new HashMap<>());
            
            System.out.println("Node uuid = " + instance.getUuid().toString()
                    + " name = " + instance.getName());
            instance.start();
            zmq.ZMQ.sleep(1, TimeUnit.HOURS);
        } catch (IOException iOException) {
            iOException.printStackTrace();
        } catch (InterruptedException interruptedException) {
            if (instance != null) {
                try {
                    instance.disconnect();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }

    }

    public static void setLevel(Level targetLevel) {
        Logger root = Logger.getLogger("");
        root.setLevel(targetLevel);
        for (Handler handler : root.getHandlers()) {
            handler.setLevel(targetLevel);
        }
        System.out.println("level set: " + targetLevel.getName());
    }

}
