package flame;

import kvs.KVSClient;
import kvs.Row;
import tools.HTTP;
import tools.Hasher;
import tools.Partitioner;
import tools.Serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

public class FlameContextImpl implements FlameContext {
    String jarName, output = "";
    int jobID = 0;
    KVSClient kvs;

    public FlameContextImpl(String jarName, KVSClient kvs) {
        this.jarName = jarName;
        this.kvs = kvs;
    }

    public KVSClient getKVS() {
        return kvs;
    }

    public void output(String s) {
        output += s;
    }

    public FlameRDD parallelize(List<String> list) throws Exception {
        String jobID = "job_" + Hasher.hash(System.currentTimeMillis() + "" + this.jobID++);
        int i = 0;
        for (String s : list) {
            Row row = new Row(Hasher.hash(jobID + i++));
            row.put("value", s);
            kvs.putRow(jobID, row);
        }
        return new FlameRDDImpl(jobID, kvs, this);
    }

    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        System.out.println("context Impl From Table");
        String outputTable = invokeOperation("/rdd/fromTable", Serializer.objectToByteArray(lambda), tableName);
        return new FlameRDDImpl(outputTable, kvs, this);
    }

    public void setConcurrencyLevel(int keyRangesPerWorker) {

    }

    public String invokeOperation(String url, byte[] lambda, String inputTable) throws IOException {
        System.out.println("context Impl Invoke Operation");
        String outputTable = "job_" + Hasher.hash(System.currentTimeMillis() + "" + this.jobID++);
        Partitioner partitioner = new Partitioner();
        for (int i = 0; i < kvs.numWorkers() - 1; i++) {
            partitioner.addKVSWorker(kvs.getWorkerAddress(i), kvs.getWorkerID(i), kvs.getWorkerID(i + 1));
            System.out.println(kvs.getWorkerAddress(i) + " " + kvs.getWorkerID(i) + " " + kvs.getWorkerID(i + 1));
        }
        partitioner.addKVSWorker(kvs.getWorkerAddress(kvs.numWorkers() - 1), kvs.getWorkerID(kvs.numWorkers() - 1), "");
        partitioner.addKVSWorker(kvs.getWorkerAddress(kvs.numWorkers() - 1), "", kvs.getWorkerID(0));
        for (String x : Coordinator.getWorkers()) {
        	partitioner.addFlameWorker(x);
        }
        Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
        for (Partitioner.Partition partition : partitions) {
            System.out.println(partition);
        }
        ArrayList<sendRequest> threads = new ArrayList<>();
        try {
            for (Partitioner.Partition partition : partitions) {
                sendRequest t = new sendRequest(outputTable, partition, "http://" + partition.assignedFlameWorker + url, lambda, inputTable);
                t.start();
                threads.add(t);
            }
            for (sendRequest t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return outputTable;
    }

    class sendRequest extends Thread {
        String outputTable, url, inputTable;
        Partitioner.Partition partition;
        byte[] lambda;

        public sendRequest(String outputTable, Partitioner.Partition partition, String url, byte[] lambda, String inputTable) {
            this.outputTable = outputTable;
            this.partition = partition;
            this.url = url;
            this.inputTable = inputTable;
            this.lambda = lambda;
        }

        @Override
        public void run() {
            try {
                if (url.contains("?")) {
                    url += "&";
                } else {
                    url += "?";
                }
//                if (!partition.fromKey.isEmpty()) {
//                    url += "start=" + partition.fromKey + "&";
//                }
//                if (!partition.toKeyExclusive.isEmpty()) {
//                    url += "end=" + partition.toKeyExclusive + "&";
//                }
                url += "inputTable=" + inputTable + "&outputTable=" + outputTable + "&coordinator=" + kvs.getCoordinator() + "&jarName=" + jarName;
                System.out.println("URL: " + url);
                HTTP.doRequest("POST", url, lambda);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
