package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PageLruCache {
    protected HashMap<PageId, Node> pid2node;
    protected int MAX_SIZE;
    protected Node head;
    protected Node tail;

    public PageLruCache(int MAX_SIZE) {
        this.MAX_SIZE = MAX_SIZE;
        pid2node = new HashMap<>(MAX_SIZE);
        head = new Node(null, null);
    }

    //删除节点
    protected void unlink(Node node) {
        //如果不最后一个结点
        if (node.next != null) {
            node.next.prev = node.prev;
        }
        node.prev.next = node.next;
    }

    //将节点插入头
    protected void linkFirst(Node node) {
        Node oldFront = this.head.next;//先得到当前头结点
        this.head.next = node;
        node.prev = this.head;
        node.next = oldFront;

        if (oldFront == null) {//防止访问null.prev
            tail = node;
        } else {
            oldFront.prev = node;
        }
    }

    //删除最后的节点
    protected void removeTail() {
        Node newTail = tail.prev;//得到新的尾节点
        tail.prev = null;
        newTail.next = null;
        tail = newTail;
    }

    //插入节点，返回被删除的页面
    public synchronized Page put(PageId pid, Page page) throws DbException {
        //不允许插入null值
        if (pid == null || page == null) {
            throw new IllegalArgumentException();
        }

        if (isCached(pid)) {
            Node node = pid2node.get(pid);
            node.page = page;
            unlink(node);
            linkFirst(node);
            return null;
        } else {
            Page removedPage=null;
            if (pid2node.size() >= MAX_SIZE) {
                //如果已满，则从后往前遍历，删除一个干净节点
                Node node = tail;
                removedPage = node.page;
                while (removedPage.isDirty() != null) {
                    node = node.prev;
                    removedPage = node.page;
                    if (node == head) {//遍历到头结点时报错
                        //displayCache();
                        throw new DbException("all pages are dirty!");
                    }
                }
                //在链表中删除该node,以及缓存中删除page
                removePage(removedPage.getId());
                pid2node.remove(removedPage.getId());
            }
            Node node = new Node(pid, page);
            linkFirst(node);
            pid2node.put(pid, node);
            return removedPage;
        }
    }

    //删除节点
    private synchronized void removePage(PageId pid) {
        if (!isCached(pid)) {
            throw new IllegalArgumentException();
        }

        //从头结点遍历
        Node removedNode = pid2node.get(pid);

        if (removedNode == tail) {
            removeTail();//tail节点处理连接之外还需要更新tail
        } else {
            removedNode.next.prev = removedNode.prev;
            removedNode.prev.next = removedNode.next;
        }
    }

    //进行回滚
    public synchronized void reCachePage(PageId pid) {
        if (!isCached(pid)) {
            throw new IllegalArgumentException();
        }

        //访问磁盘获得该page，然后更新对应节点的page
        DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page originalPage = df.readPage(pid);
        originalPage.markDirty(false,null);
        pid2node.get(pid).page=originalPage;
    }

    //返回存在于缓存中的条目，不存在则返回null
    public synchronized Page get(PageId pid) {
        if (isCached(pid)) {
            //调整最近使用的条目
            Node node = pid2node.get(pid);
            if (tail == node && node.prev != head) {
                //如果是尾节点且其前一个不为头结点，则设其前一个节点为新的尾节点
                tail = node.prev;
            }
            unlink(node);
            linkFirst(node);
            return node.page;
        }
        return null;
    }

    public synchronized boolean isCached(PageId pid) {
        return pid2node.containsKey(pid);
    }

    //用于测试
    protected void displayCache() {
        Node node = head;
        while ((node = node.next) != null) {
            System.out.print(node.page.getId().hashCode() + "\n");
        }
        System.out.println();
    }

    /**
     * @return 当前缓存的所有value
     */
    public Iterator<Page> iterator() {
        return new LruIter();
    }

    protected class Node {
        Node prev;
        Node next;
        PageId pid;
        Page page;

        Node(PageId pid, Page page) {
            this.pid = pid;
            this.page = page;
        }
    }

    protected class LruIter implements Iterator<Page> {
        Node n = head;

        @Override
        public synchronized boolean hasNext() {
            return n.next != null;
        }

        @Override
        public synchronized Page next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n = n.next;
            return n.page;
        }
    }
}
