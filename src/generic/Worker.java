package generic;


import tools.HTTP;
import java.io.IOException;
import tools.HTTP;

public class Worker {

   static int workerPort, coordinatorPort;
   protected static String storageDirectory;
   static String coordinatorIp;
   static String workerID;
    public static void startPingThread(final String addr, final String workerId, final int port) {
       (new Thread("Ping thread") {
          public void run() {
             while(true) {
                try {
                   sleep(5000);
                   HTTP.Response resp = HTTP.doRequest("GET", "http://" + addr + "/ping?id=" + workerId  + "&port=" + port, null);
                } catch (Exception e) {
                   System.err.println("Ping failed to coordinator :" + addr);
                }
             }
          }
       }).start();
    }
 }