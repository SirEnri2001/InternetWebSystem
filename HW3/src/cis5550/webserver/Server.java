package cis5550.webserver;
import cis5550.tools.Logger;

import java.io.*;
import java.net.*;
import java.util.UUID;
import java.util.concurrent.*;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;

class SessionDaemon implements Runnable{
    SessionImpl session;
    public SessionDaemon(SessionImpl sessionImpl){
        session = sessionImpl;
    }
    @Override
    public void run() {
        while(true){
            try {
                Thread.sleep(session.lastAccessTime+session.maxTimeInterval-System.currentTimeMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if(session.lastAccessTime+session.maxTimeInterval-System.currentTimeMillis()<0){
                session.invalidate();
                return;
            }
        }

    }
}
public class Server {
    public static Server serverInstance = null;
    public static boolean isServerRunning = false;
    private static int portNumber = 80;
    private static int tlsPortNumber = 443;
    private static String location = "./test/";

    private ConcurrentHashMap<String[], Route> getRoutesMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String[], Route> putRoutesMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String[], Route> postRoutesMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Session> idSessions = new ConcurrentHashMap<>();

    public void putSession(String sessionId, Session session){
        if(session==null){
            idSessions.remove(sessionId);
        }else{
            idSessions.put(sessionId, session);
        }
    }

    public  Session getSession(Request req){
        RequestImpl reqImpl = ((RequestImpl)req);
        String sessionId = reqImpl.getSessionId();
        if (sessionId==null || sessionId.isEmpty() || idSessions.get(sessionId)==null){
            UUID uuid = UUID.randomUUID();
            sessionId = uuid.toString();
            SessionImpl sessionImpl = new SessionImpl(this);
            sessionImpl.sessionId = sessionId;
            idSessions.put(sessionId, sessionImpl);
            reqImpl.createSession = true;
            reqImpl.sessionId = sessionId;
            Thread t = new Thread(new SessionDaemon(sessionImpl));
            t.start();
        }
        SessionImpl session = (SessionImpl) idSessions.get(sessionId);
        session.updateLastAccessTime();
        return session;
    }

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

    public static void securePort(int port){
        tlsPortNumber = port;
    }

    private final Logger logger = Logger.getLogger(Server.class);

    public static void run() throws Exception {
        serverInstance.startServer(portNumber);
    }

    public static void runTls() throws Exception {
        serverInstance.startTlsServer(tlsPortNumber);
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
            Thread t1 = new Thread(() -> {
                try {
                    runTls();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            t1.start();
        }
    }

    public void startServer(int port) throws Exception {
        ServerSocket ssock = null;
        int NUM_WORKERS = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        try{
            ssock = new ServerSocket(port);
        }catch (IOException ioe){
            logger.error("FATAL cannot start server socket");
            throw ioe;
        }
        while(true){
            Socket socket = ssock.accept();
            executorService.submit(new RequestHandler(socket, logger, this, getRoutesMap,putRoutesMap, postRoutesMap));
        }
    }

    public void startTlsServer(int port) throws Exception {
        String pwd = "secret";
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, pwd.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        ServerSocketFactory factory = sslContext.getServerSocketFactory();
        ServerSocket serverSocketTLS = factory.createServerSocket(tlsPortNumber);
        int NUM_WORKERS = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        while(true){
            Socket socket = serverSocketTLS.accept();
            executorService.submit(new RequestHandler(socket, logger, this, getRoutesMap,putRoutesMap, postRoutesMap));
        }
    }

    public static void main(String args[]) throws Exception {
        port(80);
        securePort(443);
        get("/echo/:x", (req,res) -> { return req.params("x"); });
        get("/session", (req,res) -> {
            Session s = req.session();
            if (s == null)
                return "null";
            return s.id(); });
        get("/perm/:x", (req,res) -> { Session s = req.session(); s.maxActiveInterval(1); if (s.attribute("test") == null) s.attribute("test", req.params("x")); return s.attribute("test"); });

        get("/", (req,res) -> { return "Hello World - this is Xinghua Han"; });
    }
}
