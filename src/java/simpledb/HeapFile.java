package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private TupleDesc tupleDesc;

    private File file;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        byte[] data = new byte[BufferPool.getPageSize()];

        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(pos);
            raf.read(data, 0, BufferPool.getPageSize());
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        //利用RandomAccessFile对磁盘文件进行写入
        byte[] data = page.getPageData();
        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "rw")) {
            int pos = page.getId().getPageNumber() * BufferPool.getPageSize();
            raf.seek(pos);
            raf.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to add.  This tuple should be updated to reflect that
     *            it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                modifiedPages.add(page);
                break;
            }
            Database.getBufferPool().releasePage(tid,pid);//此时已经可以释放锁了
        }

        //如果modifiedPages为空，则说明当前所有堆页已满，需要再增加一个堆页
        if (modifiedPages.isEmpty()) {
            HeapPageId pid = new HeapPageId(getId(), numPages());
            HeapPage newDiskPage = new HeapPage(pid, HeapPage.createEmptyPageData());
            writePage(newDiskPage);
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            modifiedPages.add(newPage);
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        //如果page的PgNo小于HeapFile的页面数，则说明含有这个页面
        PageId pid = t.getRecordId().getPageId();
        HeapPage modifiedPage = null;
        if (pid.getPageNumber() < numPages()) {
            modifiedPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            modifiedPage.deleteTuple(t);
            modifiedPage.markDirty(true, tid);
        }else{
            throw new DbException("this tuple is not in the page it's recorded");
        }

        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(modifiedPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            int pagePos;
            Iterator<Tuple> tupleIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pagePos = 0;
                HeapPageId pid = new HeapPageId(getId(), pagePos);
                Page page=Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                tupleIterator = ((HeapPage) Database.getBufferPool()
                        .getPage(tid, pid, Permissions.READ_ONLY)).iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tupleIterator == null) {
                    return false;
                }

                if (tupleIterator.hasNext()) {
                    return true;
                }

                if (++pagePos < numPages()) {
                    HeapPageId pid = new HeapPageId(getId(), pagePos);
                    tupleIterator = ((HeapPage) Database.getBufferPool()
                            .getPage(tid, pid, Permissions.READ_ONLY)).iterator();
                    return tupleIterator.hasNext();
                }

                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("not opened or no tuple remained");
                }
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pagePos = 0;
                tupleIterator = null;
            }
        };
    }

}

