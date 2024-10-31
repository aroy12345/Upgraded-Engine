package kvs;

import java.io.IOException;
import java.net.InetSocketAddress;

import webserver.Route;
import webserver.Server;


public class Coordinator extends generic.Coordinator {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Error : Usage");
            System.exit(1);
        }


        Server.port(Integer.parseInt(args[0]));

        // Register routes
        registerRoutes();
        Server.get("/", (req,res) -> { return workerTable(); });
    }


}
	