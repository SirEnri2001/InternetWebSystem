package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;

import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {
    private static KVSClient kvsClient;

    public static void main(String args[]) throws IOException {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, "" + port, port);

        HTTP.Response resp = HTTP.doRequest("GET", "http://" + args[1] + "/kvs", null);
        kvsClient = new KVSClient(new String(resp.body()));
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });
        post("/rdd/flatMap", (request, response) -> {
            String resTable = request.queryParams("resTable");
            String fromTable = request.queryParams("fromTable");
            if (resTable == null || fromTable == null) {
                response.status(400, "No result table name specified");
            }
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            Iterator<Row> rowIterator = kvsClient.scan(fromTable, fromKey, toKeyExclusive);
            for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
                Row row = it.next();
                Iterable<String> opRes = lambda.op(row.get("value"));
                if (opRes == null) {
                    continue;
                }
                for (String string : lambda.op(row.get("value"))) {
                    String rowName = Hasher.hash(UUID.randomUUID().toString());
                    kvsClient.put(resTable, rowName, "value", string);
                }
            }
            return "OK";
        });
        post("/rdd/mapToPair", (request, response) -> {
            String resTable = request.queryParams("resTable");
            String fromTable = request.queryParams("fromTable");
            if (resTable == null || fromTable == null) {
                response.status(400, "No result table name specified");
            }
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            Iterator<Row> rowIterator = kvsClient.scan(fromTable, fromKey, toKeyExclusive);
            for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
                Row row = it.next();
                FlamePair opRes = lambda.op(row.get("value"));
                if (opRes == null) {
                    continue;
                }
                String rowName = Hasher.hash(UUID.randomUUID().toString());
                kvsClient.put(resTable, rowName, opRes.a, opRes.b);
            }
            return "OK";
        });
        post("/rdd/foldByKey", (request, response) -> {
            String resTable = request.queryParams("resTable");
            String fromTable = request.queryParams("fromTable");
            if (resTable == null || fromTable == null) {
                response.status(400, "No result table name specified");
            }
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String zeroElement = URLDecoder.decode(request.queryParams("zeroElement"));
            FlamePairRDD.TwoStringsToString lambda =
                    (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            Iterator<Row> rowIterator = kvsClient.scan(fromTable, fromKey, toKeyExclusive);
            HashMap<String, String> keyAccumulator = new HashMap<>();
            for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
                Row row = it.next();
                for(String column : row.columns()){
                    if(keyAccumulator.get(column)==null){
                        keyAccumulator.put(column, zeroElement);
                    }
                    keyAccumulator.put(column, lambda.op(keyAccumulator.get(column), row.get(column)));
                }
            }
            for(String key : keyAccumulator.keySet()){
                kvsClient.put(resTable, fromTable, key, keyAccumulator.get(key));
            }

            return "OK";
        });
    }
}
