package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class FlamePairRDDImpl implements FlamePairRDD{
    String tablename;
    KVSClient kvsClient;
    FlamePairRDDImpl(String tablename, String kvsCoordinator) {
        this.tablename = tablename;
        kvsClient = new KVSClient(kvsCoordinator);
    }
    @Override
    public List<FlamePair> collect() throws Exception {
        Iterator<Row> rows = kvsClient.scan(tablename);
        LinkedList<FlamePair> result = new LinkedList<>();
        for (Iterator<Row> it = rows; it.hasNext(); ) {
            Row row = it.next();
            for(String column : row.columns()){
                result.add(new FlamePair(column, row.get(column)));
            }
        }
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String resultTable = Coordinator.flameContextImpl.invokeOperation(
                tablename, "foldByKey", Serializer.objectToByteArray(lambda), zeroElement
        );
        return new FlamePairRDDImpl(resultTable, kvsClient.getCoordinator());
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
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String resultTable = Coordinator.flameContextImpl.invokeOperation(
                tablename, "flatMap/Pair", Serializer.objectToByteArray(lambda), null
        );
        return new FlameRDDImpl(resultTable, kvsClient.getCoordinator());
    }

    @Override
    public void destroy() throws Exception {

    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String resultTable = Coordinator.flameContextImpl.invokeOperation(
                tablename, "flatMapToPair/Pair", Serializer.objectToByteArray(lambda), null
        );
        return new FlamePairRDDImpl(resultTable, kvsClient.getCoordinator());
    }

    List<FlamePair> fullJoin(Row a, String bTableName) throws IOException {
        LinkedList<FlamePair> list = new LinkedList<>();
        for(String aCol : a.columns()){
            for (Iterator<Row> it = kvsClient.scan(bTableName); it.hasNext(); ) {
                Row row = it.next();
                String bVal = row.get(aCol);
                if(bVal!=null){
                    list.add(new FlamePair(aCol, a.get(aCol) + "," + bVal));
                }
            }
        }
        return list;
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        Iterator<Row> aTableRowIter = kvsClient.scan(tablename);
        String bTableName = ((FlamePairRDDImpl)other).tablename;
        String retTableName = Hasher.hash(UUID.randomUUID().toString());
        for(;aTableRowIter.hasNext();){
            Row aRow = aTableRowIter.next();
            for(FlamePair pair : fullJoin(aRow, bTableName)){
                kvsClient.put(retTableName, Hasher.hash(UUID.randomUUID().toString()), pair.a, pair.b);
            }
        }
        return new FlamePairRDDImpl(retTableName, kvsClient.getCoordinator());
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        return null;
    }
}
