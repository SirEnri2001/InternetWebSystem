package cis5550.kvs;

import cis5550.webserver.Server;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker {

    String idPath = "";

    void run(int port, String id, String ip) {
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
            if (table == null || row == null || column == null) {
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
            if (table == null || row == null || column == null) {
                response.status(400, "Bad request");
                return "";
            }
            byte[] data = get(table, row, column);
            if (data == null) {
                response.status(404, "Not found");
            }
            response.bodyAsBytes(data);
            return null;
        });
        Server.get("/", (request, response) -> {
            String html = "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<style>\n" +
                    "table, th, td {\n" +
                    "  border:1px solid grey;\n" +
                    "}\n" +
                    "</style>\n" +
                    "<body>%1$s\n" +
                    "</body>\n" +
                    "</html>";
            StringBuilder tableData = new StringBuilder();
            for (String tableName : tableTableRow.keySet()) {
                tableData.append("<table>");
                tableData.append(String.format("<tr><td><a href=\"http://localhost:%2$d/view/%1$s\">%1$s</a></td></tr>", tableName, port));
                tableData.append("</table>");
            }
            return String.format(html, tableData);
        });
        Server.get("/view/:table", (request, response) -> {
            int fromRow = 0;
            try{
                fromRow = Integer.parseInt(request.queryParams("fromRow"));
            }catch (Exception e){
                fromRow = 0;
            }
            String tableName = request.params("table");
            if(loadTable(tableName, false)==null){
                response.status(404, "No such table");
                return null;
            }

            StringBuilder tableData = new StringBuilder();

            tableData.append("<h2>" + tableName + "</h2>");
            tableData.append("<table>");
            ArrayList<String> rowList = new ArrayList<>(tableTableRow.get(tableName).keySet());

            String html = "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<style>\n" +
                    "table, th, td {\n" +
                    "  border:1px solid grey;\n" +
                    "}\n" +
                    "</style>\n" +
                    "<body>%1$s\n" +
                    //(fromRow<=0?"": "<a href=\"http://localhost:"+port+"/view/"+tableName+"/?fromRow="+String.valueOf(Math.max(0, fromRow-10))+"\">Previous Page</a>") +
                    (fromRow>=rowList.size()-10?"": "<a href=\"http://localhost:"+port+"/view/"+tableName+"/?fromRow="+(fromRow+10)+"\">Next Page</a>") +
                    "</body>\n" +
                    "</html>";

            rowList.sort(String::compareTo);
            for (int i = fromRow; i < Math.min(fromRow + 10, rowList.size()); i++) {
                String rowName = rowList.get(i);
                tableData.append(String.format("<tr><td>%1$s</td>", rowName));
                Row row = tableTableRow.get(tableName).get(rowName);
                for (String colName : row.columns()) {
                    tableData.append(String.format("<td>%1$s:%2$s</td>", colName, row.get(colName)));
                }
                tableData.append("</tr>");
            }
            tableData.append("</table>");
            return String.format(html, tableData);
        });
        Server.get("/data/:table/:row", (request, response) -> {
            String table = request.params("table");
            String row = request.params("row");
            Row r = getRow(table, row);
            if (r == null) {
                response.status(404, "No such row");
                return null;
            }
            response.write(r.toByteArray());
            return null;
        });
        Server.get("/data/:table", (request, response) -> {
            String table = request.params("table");
            String startRow = request.queryParams("startRow");
            String endRowExclusive = request.queryParams("endRowExclusive");
            Map<String, Row> tableRow = loadTable(table, false);
            if (tableRow == null) {
                response.status(404, "No such table");
            }
            for (String rowName : tableRow.keySet()) {
                if ((startRow == null || startRow.compareTo(rowName) <= 0) && (endRowExclusive == null || endRowExclusive.compareTo(rowName) > 0)) {
                    Row r = getRow(table, rowName);
                    response.write(r.toByteArray());
                    response.write("\n".getBytes());
                }
            }
            response.write("\n".getBytes());
            return null;
        });
        Server.get("/count/:table", (request, response) -> {
            String table = request.params("table");
            return String.valueOf(tableTableRow.get(table).keySet().size());
        });
        Server.put("/rename/:table", (request, response) -> {
            String oldName = request.params("table");
            String newName = request.body();
            if (loadTable(newName, false) != null) {
                response.status(409, "Name already occupied");
                return null;
            }
            if (oldName.startsWith("pt-") && !newName.startsWith("pt-") || !oldName.startsWith("pt-") && newName.startsWith("pt-")) {
                response.status(400, "Cannot rename a pt with non pt / Cannot rename a non pt with pt");
                return null;
            }
            if (loadTable(oldName, false) == null) {
                response.status(404, "No such table");
                return null;
            }
            tableTableRow.put(newName, tableTableRow.get(oldName));
            tableTableRow.remove(oldName);
            if (oldName.startsWith("pt-")) {
                try{
                    File tableFile = new File("./__worker/" + oldName);
                    tableFile.renameTo(new File("./__worker/" + newName));
                }catch (Exception e){ }
            }
            return "OK";
        });
        Server.put("/delete/:table", (request, response) -> {
            String tableName = request.params("table");

            File tableFile = new File("./__worker/" + tableName);
            if (tableTableRow.get(tableName) == null || !tableFile.exists()) {
                response.status(404, "No such table");
                return null;
            }
            tableTableRow.remove(tableName);
            try{
                tableFile.renameTo(new File("./__worker/" + "___deleted___"+tableName));
            }catch (Exception e){ }
            return "OK";
        });
    }


    Map<String, Map<String, Row>> tableTableRow = new ConcurrentHashMap<>();

    public Map<String, Row> initTableFromFile(String table) {
        Map<String, Row> tableRow = new HashMap<>();
        String[] rowFiles = null;
        try{
            File directory = new File("./__worker/" + table);
            if (!directory.exists()) {
                return null;
            } else {
                rowFiles = directory.list();

            }
        }catch (Exception e){

        }
        for (String rowName : rowFiles) {
            try {
                FileInputStream fileInputStream = new FileInputStream("./__worker/" + table + "/" + rowName);
                tableRow.put(rowName, Row.readFrom(fileInputStream));
                fileInputStream.close();
            } catch (Exception e) {
            }
        }
        return tableRow;
    }

    public Map<String, Row> loadTable(String table, boolean create) {
        Map<String, Row> tableRow = tableTableRow.get(table);
        if (tableRow == null) {
            if (table.startsWith("pt-")) {
                tableRow = initTableFromFile(table);
            }
        }
        if (tableRow == null && create) {
            try{
                File directory = new File("./__worker/" + table);
                if (!directory.exists()) {
                    directory.mkdirs();
                }
            }catch (Exception e){

            }
            tableRow = new HashMap<>();
            tableTableRow.put(table, tableRow);
        }
        return tableRow;
    }

    public Row getRow(String table, String key) {
        Map<String, Row> tableRow = loadTable(table, false);
        if (tableRow == null) {
            return null;
        }
        Row r = tableRow.get(key);
        if (r == null) {
            return null;
        }
        return r;
    }

    public Row putRow(String table, String row) {
        Map<String, Row> tableRow = loadTable(table, true);
        Row r = tableRow.get(row);
        if (r == null) {
            r = new Row(row);
            tableRow.put(row, r);
        }
        return r;
    }

    public void put(String table, String row, String column, byte[] data) {
        putRow(table, row).put(column, data);
        Row r = putRow(table, row);
        if (table.startsWith("pt-")) {
            try {
                File rowFile = new File("./__worker/" + table + "/" + row);
                if (!rowFile.exists()) {
                    rowFile.createNewFile();
                }
                FileOutputStream fileOutputStream = new FileOutputStream(rowFile);
                fileOutputStream.write(r.toByteArray());
            } catch (Exception e) {

            }
        }
    }

    public byte[] get(String table, String row, String column) {
        Row r = getRow(table, row);
        if (r == null) {
            return null;
        }
        return r.getBytes(column);
    }

    public static void main(String[] args) throws IOException {
        File idFile = new File(args[1] + File.separator + "id");
        File directory = new File(args[1]);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        String id = "";
        if (idFile.exists()) {
            id = (new Scanner(idFile)).nextLine();
        }
        if (!idFile.exists() || id == null) {
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
