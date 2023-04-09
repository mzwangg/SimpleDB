package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private int num;
    private TupleDesc numTupleDesc;
    private Tuple numTuple;
    private boolean called;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("TupleDesc " +
                    "of child differs from table into which we are to insert");
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.num = 0;
        this.numTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.numTuple = new Tuple(numTupleDesc);
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.numTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        num = 0;
        called = false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        num = 0;
        called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        num = 0;
        called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //在InsertTest中，通过检测该函数返回值是否为null来终止循环，所以在该函数调用一遍之后就返回null
        if (called) {
            return null;
        }
        called = true;

        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
                num++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        numTuple.setField(0, new IntField(num));
        return numTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
