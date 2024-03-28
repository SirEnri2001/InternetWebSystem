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
import cis5550.webserver.Response;

class Worker extends cis5550.generic.Worker {
    private static KVSClient kvsClient;

    private static void flatMapFromRDD(Request request, Response response, final File myJAR) throws Exception {
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
            for (String opRes : lambda.op(row.get("value"))) {
                if(opRes==null){
                    continue;
                }
                kvsClient.put(resTable, Hasher.hash(UUID.randomUUID().toString()), "value", opRes);
            }
        }
    }

    private static void flatMapFromPair(Request request, Response response, final File myJAR) throws Exception {
        String resTable = request.queryParams("resTable");
        String fromTable = request.queryParams("fromTable");
        if (resTable == null || fromTable == null) {
            response.status(400, "No result table name specified");
        }
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");
        FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> rowIterator = kvsClient.scan(fromTable, fromKey, toKeyExclusive);
        for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
            Row row = it.next();
            for(String column : row.columns()){
                for (String opRes : lambda.op(new FlamePair(column, row.get(column)))) {
                    if(opRes==null){
                        continue;
                    }
                    kvsClient.put(resTable, Hasher.hash(UUID.randomUUID().toString()), "value", opRes);
                }
            }
        }
    }
    private static void flatMapToPairFromRDD(Request request, Response response, final File myJAR) throws Exception {
        String resTable = request.queryParams("resTable");
        String fromTable = request.queryParams("fromTable");
        if (resTable == null || fromTable == null) {
            response.status(400, "No result table name specified");
        }
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");
        FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> rowIterator = kvsClient.scan(fromTable, fromKey, toKeyExclusive);
        for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
            Row row = it.next();
            for (FlamePair opRes : lambda.op(row.get("value"))) {
                if(opRes==null){
                    continue;
                }
                kvsClient.put(resTable, row.key(), opRes.a, opRes.b);
            }
        }
    }

    private static void flatMapToPairFromPair(Request request, Response response, final File myJAR) throws Exception {
        String resTable = request.queryParams("resTable");
        String fromTable = request.queryParams("fromTable");
        if (resTable == null || fromTable == null) {
            response.status(400, "No result table name specified");
        }
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");
        FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> rowIterator = kvsClient.scan(fromTable, fromKey, toKeyExclusive);
        for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
            Row row = it.next();
            for(String column : row.columns()){
                for (FlamePair opRes : lambda.op(new FlamePair(column, row.get(column)))) {
                    kvsClient.put(resTable, Hasher.hash(UUID.randomUUID().toString()), opRes.a, opRes.b);
                }
            }
        }
    }

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

        post("/rdd/flatMapToPair/:inputType", (request, response) -> {
            String inputType = request.params("inputType");
            if(inputType.equals("Pair")){
                flatMapToPairFromPair(request, response, myJAR);
            }else{
                flatMapToPairFromRDD(request, response, myJAR);
            }
            return "OK";
        });
        post("/rdd/flatMap/:inputType", (request, response) -> {
            String inputType = request.params("inputType");
            if(inputType.equals("Pair")){
                flatMapFromPair(request, response, myJAR);
            }else{
                flatMapFromRDD(request, response, myJAR);
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
                byte[] valueStored = kvsClient.get(resTable, fromTable, key);
                if(valueStored==null){
                    kvsClient.put(resTable, fromTable, key, keyAccumulator.get(key));
                }else{
                    kvsClient.put(resTable, fromTable, key, lambda.op(keyAccumulator.get(key), new String(valueStored)));
                }
            }

            return "OK";
        });

        post("/rdd/fromTable", (request, response) -> {
            String resTable = request.queryParams("resTable");
            String fromTable = request.queryParams("fromTable");
            if (resTable == null || fromTable == null) {
                response.status(400, "No result table name specified");
            }
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            Iterator<Row> rowIterator = kvsClient.scan(fromTable, fromKey, toKeyExclusive);
            for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
                Row row = it.next();
                String opRes = lambda.op(row);
                if (opRes == null) {
                    continue;
                }
                kvsClient.put(resTable, row.key(), "value", opRes);
            }
            return "OK";
        });

        post("/rdd/fold", (request, response) -> {
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
            String keyAccumulator = zeroElement;
            for (Iterator<Row> it = rowIterator; it.hasNext(); ) {
                Row row = it.next();
                keyAccumulator = lambda.op(keyAccumulator, row.get("value"));
            }
            byte[] valueStored = kvsClient.get(resTable, fromTable, "value");
            if(valueStored==null){
                kvsClient.put(resTable, fromTable, "value", keyAccumulator);
            }else{
                kvsClient.put(resTable, fromTable, "value", lambda.op(keyAccumulator, new String(valueStored)));
            }
            return "OK";
        });
    }
}
