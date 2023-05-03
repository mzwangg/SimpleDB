package simpledb;

import java.util.*;

//该类根据某谓词逆序读取元组
public class BTreeReverseScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private boolean isOpen = false;
    private TransactionId tid;
    private TupleDesc myTd;
    private IndexPredicate ipred = null;
    private transient DbFileIterator it;
    private String tablename;
    private String alias;

    //构造函数
    public BTreeReverseScan(TransactionId tid, int tableid, String tableAlias, IndexPredicate ipred) {
        this.tid = tid;
        this.ipred = ipred;
        reset(tableid, tableAlias);
    }

    public String getTableName() {
        return this.tablename;
    }

    public String getAlias() {
        return this.alias;
    }

    //构造时调用，进行初始化
    public void reset(int tableid, String tableAlias) {
        this.isOpen = false;
        this.alias = tableAlias;
        this.tablename = Database.getCatalog().getTableName(tableid);

        //根据ipred是否为空判断使用reverseIterator还是indexReverseIterator
        if (ipred == null) {
            this.it = ((BTreeFile) Database.getCatalog().getDatabaseFile(tableid)).reverseIterator(tid);
        } else {
            this.it = ((BTreeFile) Database.getCatalog().getDatabaseFile(tableid)).indexReverseIterator(tid, ipred);
        }

        //将元组的名字改为tableAlias + "." + name
        myTd = Database.getCatalog().getTupleDesc(tableid);
        String[] newNames = new String[myTd.numFields()];
        Type[] newTypes = new Type[myTd.numFields()];
        for (int i = 0; i < myTd.numFields(); i++) {
            String name = myTd.getFieldName(i);
            Type t = myTd.getFieldType(i);

            newNames[i] = tableAlias + "." + name;
            newTypes[i] = t;
        }
        myTd = new TupleDesc(newTypes, newNames);
    }

    //不显示提供tableAlias的构造函数，将tableAlias设置为表名
    public BTreeReverseScan(TransactionId tid, int tableid, IndexPredicate ipred) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid), ipred);
    }

    //打开
    public void open() throws DbException, TransactionAbortedException {
        if (isOpen)
            throw new DbException("double open on one OpIterator.");

        it.open();
        isOpen = true;
    }

    public TupleDesc getTupleDesc() {
        return myTd;
    }

    //通过TupleIterator的hasnext方法判断是否还有下一个元组
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");
        return it.hasNext();
    }

    //通过TupleIterator的next方法获得下一个元组
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");

        return it.next();
    }

    //关闭
    public void close() {
        it.close();
        isOpen = false;
    }

    //重置
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        close();
        open();
    }
}


