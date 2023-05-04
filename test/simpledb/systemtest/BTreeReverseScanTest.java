package simpledb.systemtest;

import simpledb.systemtest.SystemTestUtil;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Iterator;

import org.junit.Test;
import org.junit.Before;

import simpledb.*;
import simpledb.Predicate.Op;

public class BTreeReverseScanTest extends SimpleDbTestBase {
    private final static Random r = new Random();

    private void validateScan(int[] columnSizes, int[] rowSizes)
            throws IOException, DbException, TransactionAbortedException {
        TransactionId tid = new TransactionId();
        for (int columns : columnSizes) {
            int keyField = r.nextInt(columns);//在1--columns中随机选择主键
            for (int rows : rowSizes) {
                ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
                BTreeFile f = BTreeUtility.createRandomBTreeFile(columns, rows, null, tuples, keyField);
                BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", null);
                SystemTestUtil.matchTuples(reverseScan, tuples);
                Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
            }
        }
        Database.getBufferPool().transactionComplete(tid);
    }

    //用于比较两整形数组键属性的比较器
    private static class TupleComparator implements Comparator<ArrayList<Integer>> {
        private int keyField;

        public TupleComparator(int keyField) {
            this.keyField = keyField;
        }

        //相等时返回0，小于时返回1，大于时返回-1，以实现逆序排序
        public int compare(ArrayList<Integer> t1, ArrayList<Integer> t2) {
            int cmp = 0;
            if (t1.get(keyField) < t2.get(keyField)) {
                cmp = 1;
            } else if (t1.get(keyField) > t2.get(keyField)) {
                cmp = -1;
            }
            return cmp;
        }
    }

    //记录读取页面的次数
    class InstrumentedBTreeFile extends BTreeFile {
        public InstrumentedBTreeFile(File f, int keyField, TupleDesc td) {
            super(f, keyField, td);
        }

        @Override
        public Page readPage(PageId pid) throws NoSuchElementException {
            readCount += 1;
            return super.readPage(pid);
        }

        public int readCount = 0;
    }

    //测试当表有1--4个属性时在不同元组数目下的扫描结果是否正确
    @Test
    public void testSmall() throws IOException, DbException, TransactionAbortedException {
        int[] columnSizes = new int[]{1, 2, 3, 4};
        int[] rowSizes =
                new int[]{0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + r.nextInt(4096)};
        validateScan(columnSizes, rowSizes);
    }

    //通过遍历部分元组再rewind然后再遍历部分元组，实现对rewind方法的测试
    @Test
    public void testRewind() throws IOException, DbException, TransactionAbortedException {
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = r.nextInt(2);
        BTreeFile f = BTreeUtility.createRandomBTreeFile(2, 1000, null, tuples, keyField);
        Collections.sort(tuples, new BTreeReverseScanTest.TupleComparator(keyField));//此时tuples中的数组根据键属性逆序存储

        TransactionId tid = new TransactionId();
        BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", null);
        reverseScan.open();

        //遍历前100个元组，判断顺序是否正确
        for (int i = 0; i < 100; ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuples.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }

        reverseScan.rewind();//重置迭代器

        //重新遍历前100个元组，判断顺序是否正确，以验证rewind方法
        for (int i = 0; i < 100; ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuples.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }
        reverseScan.close();
        Database.getBufferPool().transactionComplete(tid);
    }

    //测试谓词+rewind
    @Test
    public void testRewindPredicates() throws IOException, DbException, TransactionAbortedException {
        // Create the table
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = r.nextInt(3);
        BTreeFile f = BTreeUtility.createRandomBTreeFile(3, 1000, null, tuples, keyField);
        Collections.sort(tuples, new BTreeReverseScanTest.TupleComparator(keyField));//此时tuples中的数组根据键属性逆序存储

        //测试等于谓词
        TransactionId tid = new TransactionId();
        ArrayList<ArrayList<Integer>> tuplesFiltered = new ArrayList<ArrayList<Integer>>();
        IndexPredicate ipred = new IndexPredicate(Predicate.Op.EQUALS, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        Iterator<ArrayList<Integer>> it = tuples.iterator();
        while (it.hasNext()) {//首先将满足等于谓词的数组存储在tuplesFiltered中
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        reverseScan.open();

        //遍历，判断BTreeReverseScan类是否能正确处理等于谓词
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }

        reverseScan.rewind();//重置迭代器

        //遍历，判断rewind后BTreeReverseScan类是否能正确处理等于谓词
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }
        reverseScan.close();

        //测试小于谓词
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.LESS_THAN, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while (it.hasNext()) {//首先将满足小于谓词的数组存储在tuplesFiltered中
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        reverseScan.open();

        //遍历，判断BTreeReverseScan类是否能正确处理小于谓词
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }

        reverseScan.rewind();//重置迭代器

        //遍历，判断rewind后BTreeReverseScan类是否能正确处理小于谓词
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }
        reverseScan.close();

        //测试大于谓词
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while (it.hasNext()) {//首先将满足大于谓词的数组存储在tuplesFiltered中
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        reverseScan.open();

        //遍历，判断BTreeReverseScan类是否能正确处理大于谓词
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }

        reverseScan.rewind();//重置迭代器

        //遍历，判断rewind后BTreeReverseScan类是否能正确处理大于谓词
        for (int i = 0; i < tuplesFiltered.size(); ++i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i).get(keyField), SystemTestUtil.tupleToList(t).get(keyField));
        }
        reverseScan.close();
        Database.getBufferPool().transactionComplete(tid);
    }

   //测试
    @Test
    public void testReadPage() throws Exception {
        // Create the table
        final int LEAF_PAGES = 30;

        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = 0;
        BTreeFile f = BTreeUtility.createBTreeFile(2, LEAF_PAGES * 502, null, tuples, keyField);
        Collections.sort(tuples, new BTreeReverseScanTest.TupleComparator(keyField));
        TupleDesc td = Utility.getTupleDesc(2);
        BTreeReverseScanTest.InstrumentedBTreeFile table = new BTreeReverseScanTest.InstrumentedBTreeFile(f.getFile(), keyField, td);
        Database.getCatalog().addTable(table, SystemTestUtil.getUUID());

        //测试等于谓词
        TransactionId tid = new TransactionId();
        ArrayList<ArrayList<Integer>> tuplesFiltered = new ArrayList<ArrayList<Integer>>();
        IndexPredicate ipred = new IndexPredicate(Predicate.Op.EQUALS, new IntField(r.nextInt(LEAF_PAGES * 502)));
        Iterator<ArrayList<Integer>> it = tuples.iterator();
        while (it.hasNext()) {//先将满足等于谓词的数组存储到tuplesFiltered中
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(reverseScan, tuplesFiltered);
        //读取的页面数应该为3或4，包括一个根指针页面、根页面以及一个或两个叶子页面
        assertTrue(table.readCount == 3 || table.readCount == 4);

        //测试小于谓词
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.LESS_THAN, new IntField(r.nextInt(LEAF_PAGES * 502)));
        it = tuples.iterator();
        while (it.hasNext()) {//先将满足小于谓词的数组存储到tuplesFiltered中
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(reverseScan, tuplesFiltered);//此处table.readCount从0变成了32
        //读取的页面包括一个根指针页面、根页面以及叶子页面
        int leafPageCount = tuplesFiltered.size() / 502;
        if (leafPageCount < LEAF_PAGES)
            leafPageCount++;
        assertEquals(leafPageCount + 2, table.readCount);

        //测试大于谓词
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(LEAF_PAGES * 502)));
        it = tuples.iterator();
        while (it.hasNext()) {//先将满足大于谓词的数组存储到tuplesFiltered中
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(reverseScan, tuplesFiltered);
        //读取的页面包括一个根指针页面、根页面以及叶子页面
        leafPageCount = tuplesFiltered.size() / 502;
        if (leafPageCount < LEAF_PAGES)
            leafPageCount++;
        assertEquals(leafPageCount + 2, table.readCount);

        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Make test compatible with older version of ant.
     */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BTreeReverseScanTest.class);
    }
}


