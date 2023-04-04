package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbFieldIndex;

    private final Type gbFieldType;

    private final int aFieldIndex;

    private final Op what;

    private TupleDesc td;

    HashMap<Field, Integer> gbField2agVal;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what!=Op.COUNT) {
            throw new IllegalArgumentException();
        }

        this.gbFieldIndex=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aFieldIndex=afield;
        this.what=what;
        this.gbField2agVal=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field gbField=null;

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

        if (tup.getField(this.aFieldIndex).getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException();
        }

        if (gbField2agVal.containsKey(gbField)) {
            gbField2agVal.put(gbField, gbField2agVal.get(gbField)+1);
        } else {
            gbField2agVal.put(gbField, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc iteratorTd=new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
        if (gbFieldIndex == Aggregator.NO_GROUPING){
            for (Map.Entry<Field, Integer> item : gbField2agVal.entrySet()) {
                Tuple tuple = new Tuple(iteratorTd);
                tuple.setField(0, new IntField(item.getValue()));
                tuples.add(tuple);
            }
        }else{
            for (Map.Entry<Field, Integer> item : gbField2agVal.entrySet()) {
                Tuple tuple = new Tuple(iteratorTd);
                tuple.setField(0, item.getKey());
                tuple.setField(1, new IntField(item.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(iteratorTd, tuples);
    }

}