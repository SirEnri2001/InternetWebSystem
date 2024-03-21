package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.*;

public class FlameContextImpl implements FlameContext{
    String kvsCoordinator;
    KVSClient kvsClient;
    private final StringBuilder outputBuilder = new StringBuilder();
    public FlameContextImpl(String name, String kvsCoordinator) {
        this.kvsCoordinator = kvsCoordinator;
        kvsClient = new KVSClient(kvsCoordinator);
    }

    // Returns the result table name
    public String invokeOperation(String fromTableName, String operationName, byte[] lambda, String zeroElement)
            throws IOException {
        String outputTableName = Hasher.hash(UUID.randomUUID().toString());
        Partitioner p = new Partitioner();
        p.setKeyRangesPerWorker(2);
        if(kvsClient.numWorkers()==1){
            p.addKVSWorker(kvsClient.getWorkerAddress(0), null, null);
        }
        else{
            for(int i = 0;i<kvsClient.numWorkers();i++){
                if(i==kvsClient.numWorkers()-1) {
                    p.addKVSWorker(kvsClient.getWorkerAddress(i), kvsClient.getWorkerID(i), null);
                    p.addKVSWorker(kvsClient.getWorkerAddress(i), null, kvsClient.getWorkerID(0));
                }else{
                    p.addKVSWorker(kvsClient.getWorkerAddress(i), kvsClient.getWorkerID(i), kvsClient.getWorkerID(i+1));
                }
            }
        }
        for(String workerAddress : Coordinator.getWorkers()){
            p.addFlameWorker(workerAddress);
        }
        Vector<Partitioner.Partition> result = p.assignPartitions();
        ExecutorService service = Executors.newFixedThreadPool(8);
        ArrayList<Future<HTTP.Response>> futureArrayList = new ArrayList<>();
        for (Partitioner.Partition x : result){
            futureArrayList.add(service.submit(()->{
                try {
                    StringBuilder queryArgs = new StringBuilder();
                    queryArgs.append("?resTable="+outputTableName);
                    queryArgs.append("&fromTable="+fromTableName);
                    if(x.fromKey!=null){
                        queryArgs.append("&fromKey="+x.fromKey);
                    }
                    if(x.toKeyExclusive!=null){
                        queryArgs.append("&toKeyExclusive="+x.toKeyExclusive);
                    }
                    if(zeroElement!=null){
                        queryArgs.append("&zeroElement="+ URLEncoder.encode(zeroElement));
                    }
                    return HTTP.doRequest("POST",
                            "http://"+x.assignedFlameWorker+"/rdd/"+operationName+queryArgs, lambda);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for(Future<HTTP.Response> future : futureArrayList){
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return outputTableName;

    }
    @Override
    public KVSClient getKVS() {
        return null;
    }

    @Override
    public void output(String s) {
        outputBuilder.append(s);
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String tablename =  Hasher.hash(UUID.randomUUID().toString());
        for(String string : list) {
            String rowname =  Hasher.hash(UUID.randomUUID().toString());
            kvsClient.put(tablename, rowname, "value", string);
        }
        FlameRDDImpl flameRDD = new FlameRDDImpl(tablename, kvsCoordinator);
        return flameRDD;
    }


    public String getOutput(){
        return outputBuilder.toString();
    }
}
