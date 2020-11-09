package org.zeromq;

/**
 *
 * @author rabbah
 */
public class ZreEventType {
    public static final int HELLO = 1;
    public static final int WHISPER =2;
    public static final int SHOUT = 3;
    public static final int JOIN = 4;
    public static final int LEAVE = 5;
    public static final int PING = 6;
    public static final int PING_OK = 7;
    
    public static final int CONNECT = 10;

    public static String toString(int evtType) {
        switch(evtType) {
            case HELLO:
                return "HELLO";
            case WHISPER:
                return "WHISPER";
            case SHOUT:
                return "SHOUT";
            case JOIN:
                return "JOIN";
            case LEAVE:
                return "LEAVE";
            case PING:
                return "PING";
            case PING_OK:
                return "PING_OK";
            case CONNECT:
                return "CONNECT";
        }
        return "UNKNOWEN_EVENT_TYPE";
    }
    
    
}
