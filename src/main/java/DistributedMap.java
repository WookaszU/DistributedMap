import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import protos.MapValueProtos;
import java.io.*;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {

    private JChannel channel;
    private String name = "synchro";


    private static final Map<String, String> data = new ConcurrentHashMap<String, String>();

    public boolean containsKey(String key) {

        synchronized (data) {
            return data.containsKey(key);
        }
    }


    public String get(String key) {

        synchronized (data) {
            return data.get(key);
        }
    }


    public String put(String key, String value) {
        synchronized(data) {
            data.put(key, value);
            try {
                sendMessage(MapValueProtos.MapValue.MessageType.PUT, key, value);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Value not putted. Error while sending update.");
                data.remove(key);
            }
            return null;
        }
    }


    public String remove(String key) {
        synchronized (data) {
            String elem = data.remove(key);

            if(elem != null) {
                try {
                    sendMessage(MapValueProtos.MapValue.MessageType.REMOVE, key, elem);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else{
                System.out.println("Map does not contain this key.");
            }

            return elem;
        }
    }


    @Override
    public void getState(OutputStream output) throws Exception {
        synchronized(data) {
            Util.objectToStream(data, new DataOutputStream(output));
        }
        System.out.println("getState");
    }


    @Override
    @SuppressWarnings("unchecked")
    public void setState(InputStream input) throws Exception {
        System.out.println("setState");
        Map<String, String> map;
        map = (Map<String, String>)Util.objectFromStream(new DataInputStream(input));

        synchronized(data) {
            data.clear();
            data.putAll(map);
        }
        System.out.println(map.size() + " mapppas in chat history):");
    }


    private void joinChannel() throws Exception {


        channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.100")))
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
                //.addProtocol(new FLUSH());              //FLUSHING POCZYTAC O TYM JEST PONIZEJ  PARTYCJONOWANIA

        stack.init();

        channel.setReceiver(this);
        channel.connect(name);
        channel.getState(null, 0);  // no timeout

    }


    private void sendMessage(MapValueProtos.MapValue.MessageType msgType, String key, String value) throws Exception{

        MapValueProtos.MapValue mapValue;
        mapValue = MapValueProtos.MapValue.newBuilder()
                .setMsgType(msgType)
                .setKey(key)
                .setValue(value)
                .build();

        byte[] byteValue = mapValue.toByteArray();


        Message msg = new Message(null, null, byteValue);
        channel.send(msg);

    }


    @Override
    public void viewAccepted(View view) {
        System.out.println(view.toString());
        if(view instanceof MergeView) {
            ViewHandler viewHandler = new ViewHandler(channel, (MergeView) view);
            viewHandler.start();
        }
    }


    @Override
    public void receive(Message msg) {
        try {
            byte[] raw_stream;
            raw_stream = msg.getBuffer();
            MapValueProtos.MapValue mapValue = MapValueProtos.MapValue.parseFrom(raw_stream);

            if(mapValue.getMsgType().equals(MapValueProtos.MapValue.MessageType.PUT)) {
                synchronized (data) {
                    data.put(mapValue.getKey(), mapValue.getValue());
                }
            }
            else if(mapValue.getMsgType().equals(MapValueProtos.MapValue.MessageType.REMOVE)){
                synchronized (data) {
                    data.remove(mapValue.getKey());
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }



    private void printContent(){
        synchronized(data) {
            System.out.println("Map contains: ");
            for(String key: data.keySet()){
                System.out.println(key + "  " + data.get(key));
            }
        }
    }


    private void eventLoop() {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("> "); System.out.flush();
                String line = in.readLine().toLowerCase();
                if(line.startsWith("quit") || line.startsWith("exit")) {
                    break;
                }

                String[] tokens = line.split(" ");

                String key;
                String value;

                if(tokens[0].equals("put")){
                    key = tokens[1];
                    value = tokens[2];
                    put(key, value);
                    //System.out.println("Putted");
                }
                else if(tokens[0].equals("remove")){
                    key = tokens[1];
                    System.out.println("Removed from map: " + remove(key));
                }
                else if(tokens[0].equals("get")){
                    key = tokens[1];
                    System.out.println("Got from map: " + get(key));
                }
                else if(tokens[0].equals("contains")){
                    key = tokens[1];
                    System.out.println("Containing " + key + " : " + containsKey(key));
                }
                else if(tokens[0].equals("show")){
                    printContent();
                }
                else{
                    System.out.println("Wrong command!");
                }

            }
            catch(IOException e) {
                System.out.println("Error while reading input from console.");
            }
        }
    }


    private void start() throws Exception{
        joinChannel();
        eventLoop();
        channel.close();

    }


    public static void main(String[] args) throws Exception{
        new DistributedMap().start();
    }

}
