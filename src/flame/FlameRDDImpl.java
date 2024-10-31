package flame;

import kvs.KVSClient;
import kvs.Row;
import tools.Serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import flame.*;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FlameRDDImpl implements FlameRDD{

    String tableName;
    KVSClient kvs;
    FlameContextImpl flameContext;
    public FlameRDDImpl(String tableName, KVSClient kvs, FlameContextImpl flameContext) {
        this.tableName = tableName;
        this.kvs = kvs;
        this.flameContext = flameContext;
    }
    
    public String getTableName() {
    	return this.tableName;
    }

    public int count() throws Exception {
        return kvs.count(tableName);
    }

    public void saveAsTable(String tableNameArg) throws Exception {
        kvs.rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    public FlameRDD distinct() throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/distinct", null, tableName);
        return new FlameRDDImpl(outputTable, kvs, flameContext);
    }

    public void destroy() throws Exception {
        kvs.delete(tableName);
    }

    public Vector<String> take(int num) throws Exception {
        Iterator<Row> rows = kvs.scan(tableName);
        Vector<String> list = new Vector<>();
        int i = 0;
        while (rows.hasNext() && i++ < num) {
            Row row = rows.next();
            list.add(row.get("value"));
        }
        return list;
    }

    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/fold?zeroElement=" + zeroElement, Serializer.objectToByteArray(lambda), tableName);
        Iterator<Row> rows = kvs.scan(outputTable);
        String result = zeroElement;
        while (rows.hasNext()) {
            Row row = rows.next();
            result = lambda.op(result, row.get("value"));
        }
        return result;
    }

    public List<String> collect() throws Exception {
        Iterator<Row> rows = kvs.scan(tableName);
        List<String> list = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            list.add(row.get("value"));
        }
        return list;
    }

    public FlameRDD flatMap(FlameRDD.StringToIterable lambda) throws Exception {
    	String outputTable = flameContext.invokeOperation("/rdd/flatMap", Serializer.objectToByteArray(lambda), tableName);
        return new FlameRDDImpl(outputTable, kvs, flameContext);
    }

    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/flatMapToPair", Serializer.objectToByteArray(lambda), tableName);
        return new FlamePairRDDImpl(outputTable, kvs, flameContext);
    }

    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/mapToPair", Serializer.objectToByteArray(lambda), tableName);
        return new FlamePairRDDImpl(outputTable, kvs, flameContext);
    }

    public FlameRDD intersection(FlameRDD r) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/intersection", Serializer.objectToByteArray(r), tableName);
        return new FlameRDDImpl(outputTable, kvs, flameContext);
    }

    public FlameRDD sample(double f) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/sample?f=" + f, null, tableName);
        return new FlameRDDImpl(outputTable, kvs, flameContext);
    }

    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/groupBy", Serializer.objectToByteArray(lambda), tableName);
        return new FlamePairRDDImpl(outputTable, kvs, flameContext);
    }

    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/filter", Serializer.objectToByteArray(lambda), tableName);
        return new FlameRDDImpl(outputTable, kvs, flameContext);
    }

    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/mapPartitions", Serializer.objectToByteArray(lambda), tableName);
        return new FlameRDDImpl(outputTable, kvs, flameContext);
    }
}
