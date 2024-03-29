package simpledb;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private final int joinBufferSize = 16384;//对于join的缓冲区我设置了四个页面的大小，即16384字节
    private JoinPredicate joinPredicate;
    private OpIterator child1;
    private OpIterator child2;
    private TupleDesc td;
    private TupleIterator joinResults;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.joinPredicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        super.open();

        //每次open Join时生成一遍joinResult，可加快fetchNext()速度
        //simpleNestedLoopJoin()为初始版本，默认采用blockNestedLoopJoin()进行连接操作
        //this.joinResults = simpleNestedLoopJoin();
        this.joinResults = blockNestedLoopJoin();
        joinResults.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        joinResults.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        joinResults.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (joinResults.hasNext()) {
            return joinResults.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

    //以child1表为驱动表，遍历child2中的元素并根据谓词结果决定是否加入joinResult中
    //如果child2过大，每次外循环遍历时都会将child2从磁盘读取到内存，造成极大的磁盘I/O开销
    private TupleIterator simpleNestedLoopJoin() throws DbException, TransactionAbortedException {
        ArrayList<Tuple> joinTuples = new ArrayList<>();
        child1.rewind();
        while (child1.hasNext()) {
            Tuple left = child1.next();
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                if (joinPredicate.filter(left, right)) {
                    joinTuples.add(Tuple.merge(getTupleDesc(), left, right));
                }
            }
        }
        return new TupleIterator(getTupleDesc(), joinTuples);
    }

    //该方法将child1分块逐步缓存到内存中，然后以child2作为驱动表，遍历child1中对应的一块
    //使内循环中的每个元组只需要一次磁盘I/O操作，极大提高了性能
    private TupleIterator blockNestedLoopJoin() throws DbException, TransactionAbortedException {
        ArrayList<Tuple> joinTuples = new ArrayList<>();
        int maxCacheTupleNum = joinBufferSize / child1.getTupleDesc().getSize();//根据缓存大小和child1的TupleDesc大小计算缓存的元组数目
        Tuple[] cacheBlock = new Tuple[maxCacheTupleNum];
        int cacheTupleNum = 0;
        child1.rewind();

        while (child1.hasNext()) {
            cacheBlock[cacheTupleNum++] = child1.next();
            if (cacheTupleNum >= maxCacheTupleNum) {
                //如果缓冲区满了，就将child2作为驱动表处理缓存的tuple，方法与simpleNestedLoopJoin()类似，
                child2.rewind();
                while (child2.hasNext()) {
                    Tuple right = child2.next();
                    for (Tuple left : cacheBlock) {
                        if (joinPredicate.filter(left, right)) {
                            joinTuples.add(Tuple.merge(getTupleDesc(), left, right));
                        }
                    }
                }
                cacheTupleNum = 0;//在逻辑上对缓存进行清空
            }
        }

        //处理缓冲区中剩下的tuple
        if (cacheTupleNum > 0) {
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                for (Tuple left : cacheBlock) {
                    if (left == null) break;//元组为null时说明已经遍历到缓存中最后的元组了
                    if (joinPredicate.filter(left, right)) {
                        joinTuples.add(Tuple.merge(getTupleDesc(), left, right));
                    }
                }
            }
        }

        return new TupleIterator(getTupleDesc(), joinTuples);
    }

}
