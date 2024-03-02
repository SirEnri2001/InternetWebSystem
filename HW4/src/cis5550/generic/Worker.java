package cis5550.generic;
import cis5550.kvs.Row;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Worker {
    protected int port;
    protected String id;
    protected String ip;

    long timestampLastAccess;



    protected void startPingThread(){
        Thread t = new Thread(() -> {
            while(true){
                try {
                    Thread.sleep(5000);
                    URL url = new URL(String.format("http://%1$s/ping?id=%2$s&port=%3$d", ip, id, port));
                    Object content = url.getContent();
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();
    }
}
