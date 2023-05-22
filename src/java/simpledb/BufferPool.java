package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    public final int NUM_PAGES;
    private final long WAIT_TIME = 2;
    private final long MAX_WAIT_TIME = 200;
    private PageLruCache lruPagesPool;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.NUM_PAGES = numPages;
        lruPagesPool = new PageLruCache(DEFAULT_PAGES);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here

        //sleep之后再次判断是否获取到锁
        long time = 0;
        while (!lockManager.acquireLock(pid, tid, perm == Permissions.READ_ONLY)) {
            try {
                Thread.sleep(WAIT_TIME);//没成功请求到页面时等待WAIT_TIME毫秒继续请求
                time += WAIT_TIME;
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
            if (time > MAX_WAIT_TIME) {//若请求总时间超过设定时间则报错
                throw new TransactionAbortedException();
            }
        }

        Page page = lruPagesPool.get(pid);
        if (page != null) {//直接命中
            return page;
        }

        //未命中，访问磁盘并将其缓存
        DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page newPage = df.readPage(pid);
        Page removedPage = lruPagesPool.put(pid, newPage);
        if (removedPage != null) {
            try {
                flushPage(removedPage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newPage;

//        //当存在该页面时直接返回
//        if(pid2page.containsKey(pid)){
//            return pid2page.get(pid);
//        }
//
//        //当缓存达到最大值时先驱逐一个页面
//        if(pid2page.size() >= NUM_PAGES){
//            evictPage();
//        }
//
//        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//        pid2page.put(pid, page);
//        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        if (!lockManager.releaseLock(pid, tid)) {
            throw new IllegalArgumentException();
        }
        ;
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.transactionComplete(tid);
        if (commit) {
            flushPages(tid);
        } else {
            rollBackPages(tid);
        }
    }

    //在需要回滚时，将页面恢复到原本的image
    public synchronized void rollBackPages(TransactionId tid) {
        Iterator<Page> it = lruPagesPool.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                lruPagesPool.reCachePage(p.getId());
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //将Tuple插入HeapFile并将影响的表设置为dirty
        DbFile hf = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affectedPages = hf.insertTuple(tid, t);
        for (Page page : affectedPages) {
            //由于BufferPoolWrightTest的测试中未将申请的page加入BufferPool，故加入此判断
            if (!lruPagesPool.isCached(page.getId())) {
                lruPagesPool.put(page.getId(), page);
            }
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile hf = (DbFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affectedPages = hf.deleteTuple(tid, t);
        for (Page page : affectedPages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> it = lruPagesPool.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null) {
                flushPage(p.getId());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        //判断是否存在该页面
        Page page = lruPagesPool.get(pid);
        if (page == null) {
            return;
        }

//        //在输入页面为脏页面时更新页面
//        if (page.isDirty() != null) {
//            try {
//                flushPage(page.getId());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }

        lruPagesPool.reCachePage(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (pid == null || !lruPagesPool.isCached(pid)) {
            return;
        }

        Page dirty_page = lruPagesPool.get(pid);
        if (dirty_page.isDirty() == null) {
            return;
        }

        DbFile hf = Database.getCatalog().getDatabaseFile(pid.getTableId());
        hf.writePage(dirty_page);
        dirty_page.markDirty(false, null);
    }

    private synchronized void flushPage(Page dirty_page) throws IOException {
        if (dirty_page == null||dirty_page.isDirty() == null) {
            return;
        }

        DbFile hf = Database.getCatalog().getDatabaseFile(dirty_page.getId().getTableId());
        hf.writePage(dirty_page);
        dirty_page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Page> it = lruPagesPool.iterator();
        while (it.hasNext()) {
            Page page = it.next();
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        //该函数功能已经在PageLruCache类中实现
    }
}