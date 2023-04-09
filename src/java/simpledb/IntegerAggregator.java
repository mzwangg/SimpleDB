package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbFieldIndex;
    private final Type gbFieldType;
    private final int aFieldIndex;
    private final Op what;
    private HashMap<Field, Integer> gbField2agVal;
    private HashMap<Field, Integer[]> gbField2countAndSum;//用于平均值聚合时存储当前的和以及个数，以计算平均值
    private TupleDesc td;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aFieldIndex = afield;
        this.what = what;
        this.gbField2agVal = new HashMap<>();
        this.gbField2countAndSum = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field gbField = null;
        Field aggreField = tup.getField(this.aFieldIndex);

        //td将根据第一个TupleDesc进行赋值
        if (td == null) {
            td = tup.getTupleDesc();
        }

        //输入数据的TupleDesc必须一致
        if (!td.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException();
        }

        //如果要分组的话，就给gbField赋值
        if (this.gbFieldIndex != Aggregator.NO_GROUPING) {
            gbField = tup.getField(gbFieldIndex);
        }

        //该类用于Integer的聚合操作
        if (aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException();
        }

        int aggreFieldVal = ((IntField) aggreField).getValue();
        int newVal;

        if (this.what == Op.AVG) {
            if (gbField2agVal.containsKey(gbField)) {//如果之前已经读取过数据
                Integer[] countAndSum = gbField2countAndSum.get(gbField);
                countAndSum[0]++;
                countAndSum[1] += aggreFieldVal;
            } else {//如果第一次读取数据
                gbField2countAndSum.put(gbField, new Integer[]{1, aggreFieldVal});
            }
            //计算平均值
            Integer[] countAndSum = gbField2countAndSum.get(gbField);
            gbField2agVal.put(gbField, countAndSum[1] / countAndSum[0]);
            return;
        }

        //对除了均值的其他聚合操作进行处理
        if (gbField2agVal.containsKey(gbField)) {//如果之前已经读取过数据
            Integer oldVal = gbField2agVal.get(gbField);
            newVal = calcuNewValue(oldVal, aggreFieldVal);
        } else if (this.what == Op.COUNT) {//如果第一次读取数据且需要计算个数
            newVal = 1;
        } else {//如果第一次读取数据求不是计算个数
            newVal = aggreFieldVal;
        }
        gbField2agVal.put(gbField, newVal);
    }

    //根据当前值和操作计算下一个值
    private int calcuNewValue(int oldVal, int aggreFieldVal) {
        return switch (this.what) {
            case COUNT -> oldVal + 1;
            case MAX -> Math.max(oldVal, aggreFieldVal);
            case MIN -> Math.min(oldVal, aggreFieldVal);
            case SUM -> oldVal + aggreFieldVal;
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbFieldIndex == Aggregator.NO_GROUPING) {
            //如果没有分组，则返回一个(聚合值)元组
            TupleDesc iteratorTd = new TupleDesc(new Type[]{Type.INT_TYPE});
            for (Map.Entry<Field, Integer> item : gbField2agVal.entrySet()) {
                Tuple tuple = new Tuple(iteratorTd);
                tuple.setField(0, new IntField(item.getValue()));
                tuples.add(tuple);
            }
            return new TupleIterator(iteratorTd, tuples);
        } else {
            //如果需要分组，则返回一个(组别、聚合值)元组
            TupleDesc iteratorTd = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
            for (Map.Entry<Field, Integer> item : gbField2agVal.entrySet()) {
                Tuple tuple = new Tuple(iteratorTd);
                tuple.setField(0, item.getKey());
                tuple.setField(1, new IntField(item.getValue()));
                tuples.add(tuple);
            }
            return new TupleIterator(iteratorTd, tuples);
        }
    }

}
