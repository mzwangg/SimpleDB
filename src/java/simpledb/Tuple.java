package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;

    private Field[] fields;

    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        if (td.numFields() == 0) {
            throw new IllegalArgumentException();
        }
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0 || i > tupleDesc.numFields()) {
            throw new IllegalArgumentException();
        }
        fields[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i > tupleDesc.numFields()) {
            throw new IllegalArgumentException();
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder result = new StringBuilder();
        for (Field item : fields) {
            result.append(item.toString()).append("\t");
        }
        result.deleteCharAt(result.length() - 1);
        return result.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return new Iterator<Field>() {

            private int cur = 0;

            @Override
            public boolean hasNext() {
                return cur + 1 < fields.length;
            }

            @Override
            public Field next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return fields[cur++];
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        tupleDesc = new TupleDesc(td.getTdAr());
    }

    public static Tuple merge(TupleDesc td,Tuple left,Tuple right)
    {
        Tuple result=new Tuple(td);
        int leftLenth=left.getTupleDesc().numFields();
        int rightLenth=right.getTupleDesc().numFields();
        for (int i = 0; i < leftLenth; i++) {
            result.setField(i, left.getField(i));
        }
        for (int i = 0; i < rightLenth; i++) {
            result.setField(i+leftLenth , right.getField(i));
        }
        return result;
    }
}
