package simpledb;

import java.util.concurrent.ConcurrentHashMap;

public class BufferPoolList {
    private final int MAX_SIZE;//缓冲池的最大页面数
    private final int newBlocksSize;//新生代的页面数目
    private final long OLD_BLOCK_TIME;//老生代的等待时间，在该时间之后在访问某老生代节点时就会移动到新生代

    private int size;
    private boolean initial;
    private BufferPoolListNode front;
    private BufferPoolListNode middle;
    private BufferPoolListNode tail;
    private ConcurrentHashMap<PageId, BufferPoolListNode> pid2node;//通过这个映射判断某页面是否在缓冲池中

    public BufferPoolList(int maxSize, double newBlocksRate, long oldBlocksTime) {
        this.MAX_SIZE = maxSize;
        this.OLD_BLOCK_TIME = oldBlocksTime;
        this.newBlocksSize = (int) (maxSize * newBlocksRate);
        this.initial = false;
        this.pid2node = new ConcurrentHashMap<>();
    }

    //将某节点移动到新生代头部
    public void moveNode2Front(BufferPoolListNode node) {
        if (node.prev != null) {
            node.prev.next = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        }
        if (node == tail) {
            tail = node.prev;
        }

        node.prev = null;
        node.next = front;
        front = node;
    }

    //将某节点移动到老生代头部
    public void moveNode2Middle(BufferPoolListNode node) {
        if (node.prev != null) {
            node.prev.next = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        }
        if (node == tail) {
            tail = node.prev;
        }

        node.prev = middle.prev;
        middle.prev.next = node;
        node.next = middle;
        middle.prev = node;
        middle = node;
    }

    //插入页面
    public void push(PageId pid) {
        if (size < MAX_SIZE - 1) {//当页面未满时
            if (pid2node.containsKey(pid)) {//若存在该页面则直接移动到新生代头部
                moveNode2Front(pid2node.get(pid));
            } else {//否则在新生代头部插入一个新页面
                BufferPoolListNode newNode = new BufferPoolListNode(null, front, pid);
                if (front != null) {
                    front.prev = newNode;
                }
                front = newNode;
                pid2node.put(pid, newNode);
                ++size;
            }
        } else {
            //如果BufferPool刚满，则先计算得到middle和tail
            if (!initial) {
                BufferPoolListNode tempNode = front;
                for (int i = 1; i <= newBlocksSize; i++) {
                    tempNode = tempNode.next;
                }
                middle = tempNode;
                for (int i = newBlocksSize + 1; i < MAX_SIZE; i++) {
                    tempNode = tempNode.next;
                }
                tail = tempNode;
                initial = true;
            }

            if (pid2node.containsKey(pid)) {//如果存在该页面
                BufferPoolListNode oldNode = pid2node.get(pid);
                if (System.currentTimeMillis() - oldNode.initialTime >= OLD_BLOCK_TIME) {
                    moveNode2Front(oldNode);//若等待时间已过则移动到新生代头部
                } else {//若等待时间未过则移动到老生代头部
                    moveNode2Middle(oldNode);
                }
            } else {//如果不存在该页面，则插入节点到老生代头部
                BufferPoolListNode newNode = new BufferPoolListNode(middle.prev, middle, pid);
                middle.prev.next = newNode;
                middle.prev = newNode;
                middle = newNode;
                pid2node.put(pid, newNode);
                ++size;
            }
        }
    }

    //每次驱逐时都驱逐尾节点，当BufferPool未满时尾节点还不存在
    public PageId evictPage() {
        if (tail == null) {
            return null;
        }
        PageId evictPageId = tail.pid;
        tail = tail.prev;
        tail.next = null;
        --size;
        return evictPageId;
    }

    //BufferPoolList的节点类，包含了前后向指针、PageId以及创建时间
    public static class BufferPoolListNode {
        BufferPoolListNode prev;
        BufferPoolListNode next;
        PageId pid;
        long initialTime;

        public BufferPoolListNode(BufferPoolListNode prev, BufferPoolListNode next, PageId pid) {
            this.prev = prev;
            this.next = next;
            this.pid = pid;
            this.initialTime = System.currentTimeMillis();
        }
    }

}
