package flame;

import kvs.KVSClient;
import kvs.Row;
import tools.Serializer;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlamePairRDDImpl implements FlamePairRDD {
    String tableName;
    KVSClient kvs;
    FlameContextImpl flameContext;

    public FlamePairRDDImpl(String tableName, KVSClient kvs, FlameContextImpl flameContext) {
        this.tableName = tableName;
        this.kvs = kvs;
        this.flameContext = flameContext;
    }

    public List<FlamePair> collect() throws Exception {
        Iterator<Row> rows = kvs.scan(tableName);
        ArrayList<FlamePair> list = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            for (String column : row.columns()) {
                list.add(new FlamePair(row.key(), row.get(column)));
            }
        }
        return list;
    }

    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/foldByKey?zeroElement=" + URLEncoder.encode(zeroElement), Serializer.objectToByteArray(lambda), tableName);
        return new FlamePairRDDImpl(outputTable, kvs, flameContext);
    }

    public void saveAsTable(String tableNameArg) throws Exception {
        kvs.rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/pairFlatMap", Serializer.objectToByteArray(lambda), tableName);
        return new FlameRDDImpl(outputTable, kvs, flameContext);
    }

    public void destroy() throws Exception {

    }

    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/pairFlatMapToPair", Serializer.objectToByteArray(lambda), tableName);
        return new FlamePairRDDImpl(outputTable, kvs, flameContext);
    }

    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/join", ((FlamePairRDDImpl) other).tableName.getBytes(), tableName);
        return new FlamePairRDDImpl(outputTable, kvs, flameContext);
    }

    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        String outputTable = flameContext.invokeOperation("/rdd/cogroup", ((FlamePairRDDImpl) other).tableName.getBytes(), tableName);
        return new FlamePairRDDImpl(outputTable, kvs, flameContext);
    }
}
