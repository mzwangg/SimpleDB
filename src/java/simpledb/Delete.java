package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int num;
    private TupleDesc numTupleDesc;
    private Tuple numTuple;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.num=0;
        this.numTupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE});
        this.numTuple= new Tuple(numTupleDesc);
        this.called=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.numTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        num=0;
        called=false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        num=0;
        called=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        num=0;
        called=false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //在InsertTest中，通过检测该函数返回值是否为null来终止循环，所以在该函数调用一遍之后就返回null
        if(called){
            return null;
        }
        called=true;

        while(child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(tid,child.next());
                num ++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        numTuple.setField(0,new IntField(num));
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
        child=children[0];
    }

}
