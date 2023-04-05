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

    private TupleDesc td;

    HashMap<Field, Integer> gbField2agVal;

    HashMap<Field, Integer[]> gbField2countAndSum;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aFieldIndex=afield;
        this.what=what;
        this.gbField2agVal=new HashMap<>();
        this.gbField2countAndSum=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field gbField=null;
        Field aggreField=tup.getField(this.aFieldIndex);

        //The input Tuple's TupleDesc must equal this.td
        if (td == null) {
            td = tup.getTupleDesc();
        }

        if (!td.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException();
        }

        if (this.gbFieldIndex != Aggregator.NO_GROUPING) {
            gbField = tup.getField(gbFieldIndex);
        }

        if (aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException();
        }

        int aggreFieldVal=((IntField) aggreField).getValue();
        int newVal;

        if (this.what == Op.AVG) {
            if (gbField2agVal.containsKey(gbField)) {
                Integer[] countAndSum = gbField2countAndSum.get(gbField);
                countAndSum[0]++;
                countAndSum[1]+=aggreFieldVal;
            } else {
                gbField2countAndSum.put(gbField, new Integer[]{1, aggreFieldVal});
            }
            Integer[] countAndSum=gbField2countAndSum.get(gbField);
            gbField2agVal.put(gbField, countAndSum[1] / countAndSum[0]);
            return;
        }

        //aggregate operation except AVG
        if (gbField2agVal.containsKey(gbField)) {
            Integer oldVal = gbField2agVal.get(gbField);
            newVal = calcuNewValue(oldVal, aggreFieldVal);
        } else if (this.what == Op.COUNT) {//如果是对应分组的第一个参加聚合操作的tuple，那么除了count操作，其他操作结果都是待聚合值
            newVal = 1;
        } else {
            newVal = aggreFieldVal;
        }
        gbField2agVal.put(gbField, newVal);
    }

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
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbFieldIndex == Aggregator.NO_GROUPING){
            TupleDesc iteratorTd=new TupleDesc(new Type[]{Type.INT_TYPE});
            for (Map.Entry<Field, Integer> item : gbField2agVal.entrySet()) {
                Tuple tuple = new Tuple(iteratorTd);
                tuple.setField(0, new IntField(item.getValue()));
                tuples.add(tuple);
            }
            return new TupleIterator(iteratorTd, tuples);
        }else{
            TupleDesc iteratorTd=new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
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
