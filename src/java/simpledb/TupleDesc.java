package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private static final long serialVersionUID = 1L;
    //The number of fild
    private final int fildsNum;
    //The array of fild
    private final List<TDItem> tdAr;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        //typeAr must contain at least one entry
        if (typeAr.length == 0) {
            throw new IllegalArgumentException();
        }

        //The length of fieldAr must equal to the length of typeAr
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException();
        }

        fildsNum = typeAr.length;
        tdAr = new ArrayList<>();

        for (int i = 0; i < fildsNum; i++) {
            tdAr.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here

        //there is no need to determine whether the length of typeAr is zero
        // because it can be determined in TupleDesc(Type[] typeAr, String[] fieldAr)
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * Constructor. Create a new TupleDesc with tdAr
     *
     * @param tdAr_param The array of fild
     */
    public TupleDesc(List<TDItem> tdAr_param) {
        if (tdAr_param.size() == 0) {
            throw new IllegalArgumentException();
        }

        fildsNum = tdAr_param.size();
        tdAr = new ArrayList<>();

        //deep copy
        for (TDItem item : tdAr_param) {
            tdAr.add(new TDItem(item.fieldType, item.fieldName));
        }
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> newTdAr = new ArrayList<>();
        newTdAr.addAll(td1.tdAr);
        newTdAr.addAll(td2.tdAr);
        return new TupleDesc(newTdAr);
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new Iterator<TDItem>() {

            private int cur = 0;

            @Override
            public boolean hasNext() {
                return cur + 1 < numFields();
            }

            @Override
            public TDItem next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return tdAr.get(cur++);
            }
        };
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fildsNum;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > fildsNum) {
            throw new NoSuchElementException();
        }
        return tdAr.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > numFields()) {
            throw new NoSuchElementException();
        }
        return tdAr.get(i).fieldType;
    }


    //Get the tdAr of this TupleDesc
    public List<TDItem> getTdAr() {
        return tdAr;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException();
        }

        for (int i = 0; i < fildsNum; i++) {
            if (tdAr.get(i).fieldName != null && tdAr.get(i).fieldName.equals(name)) {
                return i;
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int tdSize = 0;
        for (TDItem item : tdAr) {
            tdSize += item.fieldType.getLen();
        }
        return tdSize;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        }

        if (o instanceof TupleDesc other) {
            if (numFields() != other.numFields()) {
                return false;
            }

            for (int i = 0; i < fildsNum; i++) {
                if (tdAr.get(i).fieldType != other.tdAr.get(i).fieldType) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder result = new StringBuilder();
        for (TDItem item : tdAr) {
            result.append(item.toString()).append(",");
        }
        result.deleteCharAt(result.length() - 1);
        return result.toString();
    }

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
}
