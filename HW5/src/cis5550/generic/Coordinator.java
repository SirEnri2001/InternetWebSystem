package cis5550.generic;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static cis5550.webserver.Server.*;

public class Coordinator {
    private static Map<String, Worker> idWorker;
    static long expireTime = 15000;
    public static String[] getWorkers(){
        ArrayList<String> activeWorkers = new ArrayList<>();
        for (Worker worker : idWorker.values()){
            activeWorkers.add(String.format("%1$s:%2$s", worker.ip, worker.port));
        }
        return (String[])activeWorkers.toArray();
    }

    public static String workerTable(){
        return null;
    }

    public static void garbageCollection() throws InterruptedException {
        while (true){
            LinkedList<String> deadWorkerIds = new LinkedList<>();
            for (Worker worker : idWorker.values()){
                if(System.currentTimeMillis() - worker.timestampLastAccess>expireTime){
                    deadWorkerIds.add(worker.id);
                }
            }
            for(String deadWorkerId : deadWorkerIds){
                idWorker.remove(deadWorkerId);
                workerIterator = idWorker.values().iterator();
            }
            Thread.sleep(900);
        }
    }

    public static Iterator<Worker> workerIterator;

    public static void registerRoutes(){
        idWorker = new ConcurrentHashMap<>();
        Thread t = new Thread(() -> {
            try {
                garbageCollection();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        get("/ping", (request, response) -> {
            try{
                String id = request.queryParams("id");
                int port = Integer.parseInt(request.queryParams("port"));
                if(id==null || request.queryParams("port")==null){
                    throw  new Exception();
                }
                String ip = request.ip();
                Worker worker = idWorker.get(id);
                if(worker!=null){
                    worker.port = port;
                    worker.ip = ip;
                    worker.timestampLastAccess = System.currentTimeMillis();
                }else{
                    worker = new Worker();
                    worker.port = port;
                    worker.ip = ip;
                    worker.id = id;
                    worker.timestampLastAccess = System.currentTimeMillis();
                    idWorker.put(id, worker);
                    workerIterator = idWorker.values().iterator();
                }
                return "OK";
            }catch (Exception e){
                response.status(400, "Bad request");
                return "";
            }
        });
        get("/workers", (request, response) -> {
            response.write(String.format("%d\n", idWorker.size()).getBytes());
            for (Worker worker : idWorker.values()){
                response.write(String.format("%1$s,%2$s:%3$d\n", worker.id, worker.ip, worker.port).getBytes());
            }
            return "";
        });
        get("/", (request, response) -> {
            String html = "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<style>\n" +
                    "table, th, td {\n" +
                    "  border:1px solid grey;\n" +
                    "}\n" +
                    "</style>\n" +
                    "<body>\n" +
                    "<h2>Workers</h2>\n" +
                    "<table>%1$s</table>\n" +
                    "</body>\n" +
                    "</html>";
            StringBuilder tableData = new StringBuilder();
            for (Worker worker : idWorker.values()){
                tableData.append(String.format("<tr><td>%1$s</td><td>%2$s</td><td>%3$d</td><td><a href=\"http://%2$s:%3$s\">hyperlink</a></td></tr>",
                        worker.id, worker.ip, worker.port));
            }
            return String.format(html, tableData);
        });
    }


}
