//package simpledb;
//
//import java.io.IOException;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class BufferPoolList {
//    private final int MAX_SIZE;//缓冲池的最大页面数
//    private final int newBlocksSize;//新生代的页面数目
//    private final long OLD_BLOCK_TIME;//老生代的等待时间，在该时间之后在访问某老生代节点时就会移动到新生代
//
//    private int size;
//    private boolean initial;
//    private BufferPoolListNode head;
//    private BufferPoolListNode middle;
//    private BufferPoolListNode tail;
//    private ConcurrentHashMap<PageId, BufferPoolListNode> myMap;//通过这个映射判断某页面是否在缓冲池中
//
//    public BufferPoolList(int maxSize, double newBlocksRate, long oldBlocksTime) {
//        this.MAX_SIZE = maxSize;
//        this.OLD_BLOCK_TIME = oldBlocksTime;
//        this.newBlocksSize = (int) (maxSize * newBlocksRate);
//        this.initial = false;
//        this.myMap = new ConcurrentHashMap<>();
//        this.head=null;
//    }
//
//    public synchronized void popNode(BufferPoolListNode node) {
//        if (node.prev != null) {
//            node.prev.next = node.next;
//        }
//
//        if (node.next != null) {
//            node.next.prev = node.prev;
//        }
//
//        if(node == head){
//            head=head.next;
//            if(middle!=null){
//                middle=middle.next;
//            }
//        }else if(node==middle){
//            middle=middle.next;
//        }else if (node == tail) {
//            tail = tail.prev;
//        }
//
//        --size;
//    }
//
//    //将某节点移动到新生代头部
//    public synchronized void moveNode2head(BufferPoolListNode node) {
//        popNode(node);
//
//        node.prev = null;
//        node.next = head;
//        if(head!=null){
//            head.prev=node;
//        }
//        head = node;
//
//        ++size;
//    }
//
//    //将某节点移动到老生代头部
//    public synchronized void moveNode2Middle(BufferPoolListNode node) {
//        popNode(node);
//
//        node.prev = middle.prev;
//        middle.prev.next = node;
//        node.next = middle;
//        middle.prev = node;
//        middle = node;
//
//        ++size;
//    }
//
//    //插入页面
//    public synchronized void push(PageId pid) {
//        if (size < MAX_SIZE - 1) {//当页面未满时
//            if (myMap.containsKey(pid)) {//若存在该页面则直接移动到新生代头部
//                moveNode2head(myMap.get(pid));
//            } else {//否则在新生代头部插入一个新页面
//                BufferPoolListNode newNode = new BufferPoolListNode(null, head, pid);
//                if (head != null) {
//                    head.prev = newNode;
//                }
//                head = newNode;
//                myMap.put(pid, newNode);
//                ++size;
//            }
//        } else {
//            //如果BufferPool刚满，则先计算得到middle和tail
//            if (!initial) {
//                BufferPoolListNode tempNode = head;
//                for (int i = 1; i <= newBlocksSize; i++) {
//                    tempNode = tempNode.next;
//                }
//                middle = tempNode;
//                if(tempNode==null){
//                    tail=tempNode;
//                }
//                while(tempNode.next!=null){
//                    tempNode=tempNode.next;
//                }
//
//                tail = tempNode;
//                initial = true;
//            }
//
//            if (myMap.containsKey(pid)) {//如果存在该页面
//                BufferPoolListNode oldNode = myMap.get(pid);
//                if (System.currentTimeMillis() - oldNode.initialTime >= OLD_BLOCK_TIME) {
//                    moveNode2head(oldNode);//若等待时间已过则移动到新生代头部
//                } else {//若等待时间未过则移动到老生代头部
//                    moveNode2Middle(oldNode);
//                }
//            } else {//如果不存在该页面，则插入节点到老生代头部
//                BufferPoolListNode newNode = new BufferPoolListNode(middle.prev, middle, pid);
//                middle.prev.next = newNode;
//                middle.prev = newNode;
//                middle = newNode;
//                myMap.put(pid, newNode);
//                ++size;
//            }
//        }
//    }
//
//    //每次驱逐时都驱逐尾节点，当BufferPool未满时尾节点还不存在
//    public synchronized PageId evictPage() throws DbException{
//        if (tail == null) {
//            return null;
//        }
//
//        BufferPoolListNode node=tail;
//        try {
//            while(Database.getBufferPool().bufferPageisDirty(node.pid)){//从后往前遍历，直到找到一个不是脏页面的页面
//                node=node.prev;
//                if(node==null){//当遍历到null还没找到，说明全为脏页面
//                    throw new DbException("All pages are dirty!");
//                }
//            }
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//
//        PageId evictPageId = node.pid;
//        popNode(node);
//        return evictPageId;
//    }
//
//    //BufferPoolList的节点类，包含了前后向指针、PageId以及创建时间
//    public static class BufferPoolListNode {
//        BufferPoolListNode prev;
//        BufferPoolListNode next;
//        PageId pid;
//        long initialTime;
//
//        public BufferPoolListNode(BufferPoolListNode prev, BufferPoolListNode next, PageId pid) {
//            this.prev = prev;
//            this.next = next;
//            this.pid = pid;
//            this.initialTime = System.currentTimeMillis();
//        }
//    }
//}
