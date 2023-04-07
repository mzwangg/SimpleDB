package simpledb;

import java.util.concurrent.ConcurrentHashMap;

public class BufferPoolList {
    private final int MAX_SIZE;
    private final int newBlocksSize;
    private final long OLD_BLOCK_TIME;

    private int size;
    private boolean initial;
    private BufferPoolListNode front;
    private BufferPoolListNode middle;
    private BufferPoolListNode tail;
    private ConcurrentHashMap<PageId, BufferPoolListNode> pid2node;
    public BufferPoolList(int maxSize, double newBlocksRate, long oldBlocksTime) {
        this.MAX_SIZE = maxSize;
        this.OLD_BLOCK_TIME = oldBlocksTime;
        this.newBlocksSize = (int) (maxSize * newBlocksRate);
        this.initial = false;
        this.pid2node = new ConcurrentHashMap<>();
    }

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

    public void push(PageId pid) {
        if (size < MAX_SIZE) {
            if (pid2node.containsKey(pid)) {
                moveNode2Front(pid2node.get(pid));
            } else {
                BufferPoolListNode newNode = new BufferPoolListNode(null, front, pid);
                if (front != null) {
                    front.prev = newNode;
                }
                front = newNode;
                pid2node.put(pid, newNode);
                ++size;
            }
        } else {
            //如果BufferPool刚满，则先得到middle和tail
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

            if (pid2node.containsKey(pid)) {
                BufferPoolListNode oldNode=pid2node.get(pid);
                if(System.currentTimeMillis() - oldNode.initialTime >= OLD_BLOCK_TIME) {
                    moveNode2Front(oldNode);
                }else{
                    moveNode2Middle(oldNode);
                }
            } else {
                BufferPoolListNode newNode = new BufferPoolListNode(middle.prev, middle, pid);
                middle.prev.next = newNode;
                middle.prev = newNode;
                middle = newNode;
                pid2node.put(pid, newNode);
                ++size;
            }
        }
    }

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
