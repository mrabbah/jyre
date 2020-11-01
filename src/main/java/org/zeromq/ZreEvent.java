/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.zeromq;

import java.util.UUID;

/**
 *
 * @author rabbah
 */
public class ZreEvent {

    public enum Type {
        ENTER, JOIN, LEAVE, WHISPER, SHOUT, LEADER;
    }
    
    public Type type; //  Event type 
    public Peer peer;
    public UUID peerUUID; //  Sender UUID
    public String peerName; //  Sender public name as string
    public String peerAddress; //  Sender ipaddress as string, for an ENTER event
    public byte[] headers; //  Headers, for an ENTER event
    public String group; //  Group name for a SHOUT event
    public byte[] message; //  Message payload for SHOUT or WHISPER

    public ZreEvent(byte[] data) {
        
    }
    
    
    
}
