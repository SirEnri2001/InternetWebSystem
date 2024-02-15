package cis5550.webserver;
import cis5550.tools.Logger;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
public class Server {
    public static Server serverInstance = null;
    public static boolean isServerRunning = false;
    private static int portNumber = 80;
    private static String location = "./test/";

    private ConcurrentHashMap<String[], Route> getRoutesMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String[], Route> putRoutesMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String[], Route> postRoutesMap = new ConcurrentHashMap<>();

    public static String getLocation(){
        return location;
    }

    public static int getPortNumber(){
        return portNumber;
    }

    public static class staticFiles {
        public static void location(String s) {
            Server.location = s;
        }
    }
    public static String[] validateUri(String uri){
        uri = uri.trim();
        if(uri.startsWith("/")){
            uri = uri.substring(1);
        }
        return uri.split("/+");
    }
    public static void get(String uri, Route route) {
        checkAndStartDaemon();
        serverInstance.logger.info("Get: "+uri);
        serverInstance.getRoutesMap.put(validateUri(uri), route);
    }

    public static void post(String uri, Route route) {
        checkAndStartDaemon();
        serverInstance.logger.info("Post: "+uri);
        serverInstance.postRoutesMap.put(validateUri(uri), route);
    }

    public static void put(String uri, Route route) {
        checkAndStartDaemon();
        serverInstance.logger.info("Put: "+uri);
        serverInstance.putRoutesMap.put(validateUri(uri), route);
    }

    public static void port(int portNumber) {
        Server.portNumber = portNumber;
    }

    private final Logger logger = Logger.getLogger(Server.class);

    public static void run() throws Exception {
        serverInstance.startServer(portNumber);
    }

    public static void main(String args[]) throws Exception {
        Socket s = new Socket("localhost", 8080);
        s.getOutputStream().write("GET /hello HTTP/1.1\r\n\r\n".getBytes());
        StringBuilder stringBuilder = new StringBuilder();
        while(true){
            stringBuilder.append(((char) s.getInputStream().read()));
            System.out.println(stringBuilder);
        }

    }

    // Create a instance of server and start running if not
    public static void checkAndStartDaemon(){
        if(!isServerRunning || serverInstance==null){
            serverInstance = new Server();
            isServerRunning = true;
            serverInstance.logger.info("Initialized new server");
            Thread t = new Thread(() -> {
                try {
                    run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
        }
    }

    public void startServer(int port) throws IOException {
        System.out.println("Written by Xinghua Han");
        ServerSocket ssock = null;
        int NUM_WORKERS = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        try{
            ssock = new ServerSocket(port);
        }catch (IOException ioe){
            logger.error("FATAL cannot start server socket");
        }
        assert ssock != null;
        while(true){
            Socket socket = ssock.accept();
            executorService.submit(new RequestHandler(socket, logger, getRoutesMap,putRoutesMap, postRoutesMap));
        }
    }
}
