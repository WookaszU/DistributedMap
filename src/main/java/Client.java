import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.ProtocolStack;
import protos.MapValueProtos;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Client {

    private JChannel channel;
    private String name = "synchro";


//
//    private void sendMessage(MapValueProtos.MapValue.MessageType msgType, String key, String value) throws Exception{
//
//        MapValueProtos.MapValue mapValue;
//        mapValue = MapValueProtos.MapValue.newBuilder()
//                .setMsgType(msgType)
//                .setKey(key)
//                .setValue(value)
//                .build();
//
//        byte[] byteValue = mapValue.toByteArray();
//
//
//        Message msg = new Message(null, null, byteValue);
//        channel.send(msg);
//
//    }

    private void start() throws Exception {

        channel = new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        channel.setProtocolStack(stack);
        stack.addProtocol(new UDP())
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL()
                        .setValue("timeout", 12000)
                        .setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE_TRANSFER());
        stack.init();

        channel.connect(name);

        String key = "alla";
        String value = "makota";

        //sendMessage(MapValueProtos.MapValue.MessageType.PUT, key, value);

        Thread.sleep((long)(10000));
        channel.close();
    }



    public static void main(String[] args) throws Exception {
        new Client().start();
    }
}
