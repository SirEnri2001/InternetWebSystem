package cis5550.kvs;
import static cis5550.webserver.Server.*;

import cis5550.webserver.Server;
import cis5550.webserver.Session;
public class Coordinator extends cis5550.generic.Coordinator{
    public static void main (String[] args){
        int port = 8080;
        if(args.length>0){
            port = Integer.parseInt(args[0]);
        }
        port(port);
        registerRoutes();
        get("/", (req, resp)->{ resp.write("KVS Coordinator".getBytes());return null;});
    }
}
