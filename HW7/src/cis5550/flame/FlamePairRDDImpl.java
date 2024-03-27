package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
}
