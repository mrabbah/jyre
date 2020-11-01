package org.zeromq.jyre;

import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zeromq.Jyre;
import org.zeromq.SocketType;
import org.zeromq.Utils;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZreConstants;

/**
 * JYRE MAIN APP
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, InterruptedException
    {
        setLevel(Level.INFO);
        System.out.println( "JYRE Test!" );
        Jyre instance = Jyre.getInstance();
        System.out.println("Node uuid = " + instance.getUuid().toString() + 
                " name = " + instance.getName());
        instance.start();
        zmq.ZMQ.sleep(120);
        instance.disconnect();
        
        /*InetAddress i = InetAddress.getByName("0.0.0.0");
        System.out.println(i.getHostAddress());
        ZContext context = new ZContext();
        ZMQ.Socket router = context.createSocket(SocketType.ROUTER);
        router.bind("tcp://" + ZreConstants.DEFAULT_LISTENING_INTERFACE + ":" + Utils.findOpenPort());
        System.out.println(router.getLastEndpoint());
        
        router.close();
        context.close();*/
        
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
