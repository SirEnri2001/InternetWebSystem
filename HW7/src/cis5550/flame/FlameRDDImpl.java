package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;

import java.util.*;

public class FlameRDDImpl implements FlameRDD{
    String tablename;
    KVSClient kvsClient;
    FlameRDDImpl(String tablename, String kvsCoordinator) {
        this.tablename = tablename;
        kvsClient = new KVSClient(kvsCoordinator);
    }

    @Override
    public int count() throws Exception {
        return kvsClient.count(tablename);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        Iterator<Row> rows = kvsClient.scan(tablename);
        for (Iterator<Row> it = rows; it.hasNext(); ) {
            Row row = it.next();
            for(String column : row.columns()){
                kvsClient.put(tableNameArg, row.key(), column, row.get(column));
            }
        }
        tablename = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        String retTableName = Hasher.hash(UUID.randomUUID().toString());
        String tempTableName = Hasher.hash(UUID.randomUUID().toString());
        Iterator<Row> rows = kvsClient.scan(tablename);
        for (Iterator<Row> it = rows; it.hasNext(); ) {
            Row row = it.next();
            kvsClient.put(tempTableName, row.get("value"), "value", row.key());
        }
        for (Iterator<Row> it = kvsClient.scan(tempTableName); it.hasNext(); ) {
            Row row = it.next();
            kvsClient.put(retTableName, row.get("value"), "value", row.key());
        }
        kvsClient.delete(tempTableName);
        return new FlameRDDImpl(retTableName, kvsClient.getCoordinator());
    }

    @Override
    public void destroy() throws Exception {

    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Vector<String> ret = new Vector<>();
        Iterator<Row> iterator = kvsClient.scan(tablename);
        while(iterator.hasNext() && num-->0){
            Row row = iterator.next();
            ret.add(row.get("value"));
        }
        return ret;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        String resultTable = Coordinator.flameContextImpl.invokeOperation(
                tablename, "fold", Serializer.objectToByteArray(lambda), zeroElement
        );
        return kvsClient.getRow(resultTable, tablename).get("value");
    }

    @Override
    public List<String> collect() throws Exception {
        Iterator<Row> rows = kvsClient.scan(tablename);
        LinkedList<String> result = new LinkedList<>();
        for (Iterator<Row> it = rows; it.hasNext(); ) {
            Row row = it.next();
            result.add(row.get("value"));
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String resultTable = Coordinator.flameContextImpl.invokeOperation(
                tablename, "flatMap/RDD", Serializer.objectToByteArray(lambda), null
        );
        return new FlameRDDImpl(resultTable, kvsClient.getCoordinator());
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String resultTable = Coordinator.flameContextImpl.invokeOperation(
                tablename, "flatMapToPair/RDD", Serializer.objectToByteArray(lambda), null
        );
        return new FlamePairRDDImpl(resultTable, kvsClient.getCoordinator());
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String resultTable = Coordinator.flameContextImpl.invokeOperation(
                tablename, "mapToPair", Serializer.objectToByteArray(lambda), null
        );
        return new FlamePairRDDImpl(resultTable, kvsClient.getCoordinator());
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        return null;
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return null;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        return null;
    }
}
