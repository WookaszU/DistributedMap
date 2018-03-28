import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class App {


    private DistributedMap distributedMap;

    public App(DistributedMap distributedMap) {
        this.distributedMap = distributedMap;
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

                if(tokens[0].equals("put") && tokens.length == 3){
                    key = tokens[1];
                    value = tokens[2];
                    distributedMap.put(key, value);
                }
                else if(tokens[0].equals("remove") && tokens.length == 2){
                    key = tokens[1];
                    System.out.println("Removed from map: " + distributedMap.remove(key));
                }
                else if(tokens[0].equals("get") && tokens.length == 2){
                    key = tokens[1];
                    System.out.println("Got from map: " + distributedMap.get(key));
                }
                else if(tokens[0].equals("contains") && tokens.length == 2){
                    key = tokens[1];
                    System.out.println("Containing " + key + " : " + distributedMap.containsKey(key));
                }
                else if(tokens[0].equals("show")){
                    distributedMap.printContent();
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


    public static void main(String[] args) {
        System.setProperty("java.net.preferIPv4Stack", "true");
        DistributedMap distributedMap = new DistributedMap("cluster");
        try {
            distributedMap.joinChannel();
            new App(distributedMap).eventLoop();
            distributedMap.closeChannel();
        }
        catch(Exception e){
            System.out.println("Unable to join channel!");
        }
    }
}
