package cis5550.kvs;

import static cis5550.webserver.Server.*;
import cis5550.webserver.Request;
import cis5550.webserver.Server;

import java.io.*;
import java.nio.Buffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker{

    String idPath = "";
    void run(int port, String id, String ip){
        this.port = port;
        this.id = id;
        this.ip = ip;
        startPingThread();
        System.out.println(port);
        Server.port(port);
        Server.put("/data/:table/:row/:column", (request, response) -> {
            String table = request.params("table");
            String row = request.params("row");
            String column = request.params("column");
            if(table==null || row==null || column==null){
                response.status(400, "Bad request");
                return "";
            }
            put(table, row, column, request.bodyAsBytes());
            return "OK";
        });
        Server.get("/data/:table/:row/:column", (request, response) -> {
            String table = request.params("table");
            String row = request.params("row");
            String column = request.params("column");
            if(table==null || row==null || column==null){
                response.status(400, "Bad request");
                return "";
            }
            byte[] data = get(table, row, column);
            if(data==null){
                response.status(404, "Not found");
            }
            response.bodyAsBytes(data);
            return null;
        });
    }


    Map<String, Map<String, Row>> tableTableRow = new ConcurrentHashMap<>();

    public Row getRow(String table, String key){
        Map<String, Row> tableRow = tableTableRow.get(table);
        if(tableRow==null){
            return null;
        }
        Row r = tableRow.get(key);
        if(r==null){
            return null;
        }
        return r;
    }

    public Row putRow(String table, String row){
        Map<String, Row> tableRow = tableTableRow.get(table);
        if(tableRow==null){
            //create table
            tableRow = new HashMap<>();
            tableTableRow.put(table, tableRow);
        }
        Row r = tableRow.get(row);
        if(r==null){
            r = new Row(row);
            tableRow.put(row, r);
        }
        return r;
    }

    public void put(String table, String row, String column, byte[] data){
        putRow(table, row).put(column, data);
    }

    public byte[] get(String table, String row, String column){
        return getRow(table, row).getBytes(column);
    }

    public static void main(String[] args) throws IOException {
        File idFile = new File(args[1]+File.separator+"id");
        File directory = new File(args[1]);
        if(!directory.exists()){
            directory.mkdirs();
        }
        String id = "";
        if(idFile.exists()){
            id = (new Scanner(idFile)).nextLine();
        }
        if (!idFile.exists() || id==null){
            idFile.createNewFile();
            String SALTCHARS = "abcdefghijklmnopqrstuvwxyz";
            StringBuilder idBuilder = new StringBuilder();
            Random rnd = new Random();
            while (idBuilder.length() < 5) {
                int index = (int) (rnd.nextFloat() * SALTCHARS.length());
                idBuilder.append(SALTCHARS.charAt(index));
            }
            id = idBuilder.toString();
            PrintWriter printWriter = new PrintWriter(idFile);
            printWriter.write(id);
            printWriter.flush();
        }
        Worker w = new Worker();
        w.run(Integer.parseInt(args[0]), id, args[2]);
    }
}
