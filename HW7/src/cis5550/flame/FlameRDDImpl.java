package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FlameRDDImpl implements FlameRDD{
    String tablename;
    KVSClient kvsClient;
    FlameRDDImpl(String tablename, String kvsCoordinator) {
        this.tablename = tablename;
        kvsClient = new KVSClient(kvsCoordinator);
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
                tablename, "flatMap", Serializer.objectToByteArray(lambda), null
        );
        return new FlameRDDImpl(resultTable, kvsClient.getCoordinator());
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
}
