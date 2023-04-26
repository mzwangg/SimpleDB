package simpledb;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;

    private OpIterator child;

    private TupleIterator filterResult;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();

        //每次open Filter时生成一遍filterResult，可加快fetchNext()速度
        filterResult = filter();
        filterResult.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        filterResult = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        filterResult.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (filterResult.hasNext()) {
            return filterResult.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

    private TupleIterator filter() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        //遍历child迭代器，如果满足谓词条件就加入filterResult中
        ArrayList<Tuple> filterTuples = new ArrayList<>();
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if (predicate.filter(tuple)) {
                filterTuples.add(tuple);
            }
        }
        return new TupleIterator(getTupleDesc(), filterTuples);
    }
}
