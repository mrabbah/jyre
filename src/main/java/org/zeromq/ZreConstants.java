/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.zeromq;

/**
 *
 * @author rabbah
 */
public class ZreConstants {
    public static final long DEFAULT_BROADCAST_INTERVAL = 1000L;
    public static final String DEFAULT_BROADCAST_HOST = "255.255.255.255"; // is this the source/interface address? or the broadcast address
    public static final int DEFAULT_UDP_PORT = 5670;
    public static final byte[] DEFAULT_PREFIX = new byte[]{'Z', 'R', 'E', 0x01};
    public static final int VERSION = 2;
    public static final String DEFAULT_GROUP = "GLOBAL";
    public static final String DEFAULT_LISTENING_INTERFACE = "*";
    public static final int PEER_EXPIRED = 30000; //Peer expire after 30s if he didn't contact us
    public static final int PEER_EVASIVE = 10000; // Mark evasive after 10s
    public static final int NB_MESSAGE_TO_SEND_PER_SECONDS = 100;
    
}
