package flame;

import java.util.*;
import java.io.*;

import static webserver.Server.*;

import tools.Hasher;
import tools.Serializer;
import kvs.*;

class Worker extends generic.Worker {

    public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, Integer.toString(port), port);
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        post("/rdd/flatMap", (request, response) -> {
        	try {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            if(request.bodyAsBytes() == null) {
            	System.out.println("body is null");
            }
            FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                Iterable<String> result = lambda.op(value);
                if (result != null) {
                    for (String s : result) {
                        String a = Hasher.hash(s + System.currentTimeMillis() + Math.random());
                        kvs.put(outputTable, a, "value", s);
                    }
                }
            }

        	}catch(Exception e) {
                System.out.println("Flame Worker flatMap exception");
        		e.printStackTrace();
        	}
            return "OK";
        });

        post("/rdd/mapToPair", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            System.out.println("1111");
            FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            System.out.println("IN RDD WORKER MAP TO PAIR");
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                System.out.println("value: " + value);
                FlamePair result = lambda.op(value);
                if (result != null) {
                    kvs.put(outputTable, result.a, row.key(), result.b);
                }
            }
            return "OK";
        });
        post("/rdd/foldByKey", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            String zeroElement = request.queryParams("zeroElement");
            FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                String accumulator = zeroElement;
                for (String column : row.columns()) {
                    accumulator = lambda.op(accumulator, row.get(column));
                }
                if (kvs.existsRow(outputTable, row.key())) {
                    Row temp = kvs.getRow(outputTable, row.key());
                    accumulator = lambda.op(accumulator, temp.get("value"));
                }
                kvs.put(outputTable, row.key(), "value", accumulator);
            }
            return "OK";
        });
        post("/rdd/intersection", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            FlameRDD r = (FlameRDD) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            List<String> list = r.collect();
            while (rows.hasNext()) {
                Row row = rows.next();
                if (list.contains(row.get("value"))) {
                    kvs.putRow(outputTable, row);
                }
            }
            return "OK";
        });
        post("/rdd/sample", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            double f = Double.parseDouble(request.queryParams("f"));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                if (Math.random() < f) {
                    kvs.putRow(outputTable, row);
                }
            }
            return "OK";
        });
        post("/rdd/groupBy", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            FlameRDD.StringToString lambda = (FlameRDD.StringToString) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                String key = lambda.op(value);
                kvs.put(outputTable, key, row.key(), value);
            }
            return "OK";
        });
        post("/rdd/fromTable", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            System.out.println("fromTable");
            while (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row: " + row.get(row.columns().iterator().next()));
                String res = lambda.op(row);
                if (res != null)
                    kvs.put(outputTable, row.key(), "value", res);
            }
            return "OK";
        });

        post("/rdd/flatMapToPair", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                Iterable<FlamePair> result = lambda.op(value);
                if (result != null) {
                    for (FlamePair p : result) {
                        kvs.put(outputTable, p.a, Hasher.hash(row.key() + Math.random() * System.currentTimeMillis()), p.b);
                    }
                }
            }
            return "OK";
        });
        post("/rdd/pairFlatMap", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                for (String column : row.columns()) {
                    Iterable<String> result = lambda.op(new FlamePair(row.key(), row.get(column)));
                    if (result != null) {
                        for (String s : result) {
                            String a = Hasher.hash(s + System.currentTimeMillis() + Math.random());
                            kvs.put(outputTable, a, "value", s);
                        }
                    }
                }
            }
            return "OK";
        });
        post("/rdd/pairFlatMapToPair", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            String jarName = request.queryParams("jarName");
            FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                for (String column : row.columns()) {
                    Iterable<FlamePair> result = lambda.op(new FlamePair(row.key(), row.get(column)));
                    if (result != null) {
                        for (FlamePair p : result) {
                            String a = Hasher.hash(p.a + System.currentTimeMillis() + Math.random()) + Hasher.hash(p.b + System.currentTimeMillis() + Math.random());
                            kvs.put(outputTable, p.a, a, p.b);
                        }
                    }
                }
            }
            return "OK";
        });
        post("/rdd/distinct", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                kvs.put(outputTable, row.get("value"), "value", row.get("value"));
            }
            return "OK";
        });
        post("/rdd/join", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String otherTable = new String(request.bodyAsBytes());
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                if (kvs.existsRow(otherTable, row.key())) {
                    Row other = kvs.getRow(otherTable, row.key());
                    for (String column1 : row.columns()) {
                        for (String column2 : other.columns()) {
                            kvs.put(outputTable, row.key(), Hasher.hash(column1) + "*" + Hasher.hash(column2), row.get(column1) + "," + other.get(column2));
                        }
                    }
                }
            }
            return "OK";
        });
        post("/rdd/fold", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String zeroElement = request.queryParams("zeroElement");
            String coordinator = request.queryParams("coordinator");
            String jarName = request.queryParams("jarName");
            String outputTable = request.queryParams("outputTable");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                String accumulator = zeroElement;
                for (String column : row.columns()) {
                    accumulator = lambda.op(accumulator, row.get(column));
                }
                if (kvs.existsRow(outputTable, row.key())) {
                    Row temp = kvs.getRow(outputTable, row.key());
                    accumulator = lambda.op(accumulator, temp.get("value"));
                }
                kvs.put(outputTable, row.key(), "value", accumulator);
            }
            return "OK";
        });
        post("/rdd/filter", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String jarName = request.queryParams("jarName");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            FlameRDD.StringToBoolean lambda = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                if (lambda.op(row.get("value"))) {
                    kvs.putRow(outputTable, row);
                }
            }
            return "OK";
        });
        post("/rdd/mapPartitions", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String jarName = request.queryParams("jarName");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            FlameRDD.IteratorToIterator lambda = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(request.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            ArrayList<String> sRows = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                sRows.add(row.key());
            }
            Iterator<String> result = lambda.op(sRows.iterator());
            while (result.hasNext()) {
                kvs.putRow(outputTable, kvs.getRow(inputTable, result.next()));
            }
            return "OK";
        });
        post("/rdd/cogroup", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String coordinator = request.queryParams("coordinator");
            String otherTable = new String(request.bodyAsBytes());
            String jarName = request.queryParams("jarName");
            String start = request.queryParams().contains("start") ? request.queryParams("start") : null;
            String end = request.queryParams().contains("end") ? request.queryParams("end") : null;
            KVSClient kvs = new KVSClient(coordinator);
            Iterator<Row> rows = kvs.scan(inputTable, start, end);
            while (rows.hasNext()) {
                Row row = rows.next();
                ArrayList<String> valuesA = new ArrayList<>();
                ArrayList<String> valuesB = new ArrayList<>();
                for (String column : row.columns()) {
                    valuesA.add(row.get(column));
                }
                if (kvs.existsRow(otherTable, row.key())) {
                    Row other = kvs.getRow(otherTable, row.key());
                    for (String column : other.columns()) {
                        valuesB.add(other.get(column));
                    }
                }
                kvs.put(outputTable, row.key(), "value", valuesA.toString() + valuesB);
            }
            return "OK";
        });
    }
}