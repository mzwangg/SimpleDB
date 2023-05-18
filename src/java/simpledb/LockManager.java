package simpledb;

import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    //将页面ID、事务ID映射到对应的锁,ConcurrentHashMap线程安全
    ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Lock>> pageMap;

    //构造函数，创建映射
    public LockManager() {
        pageMap = new ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Lock>>();
    }

    public synchronized boolean acquireLock(PageId pid, TransactionId tid, boolean isShared) {

        //若此页还没创建过锁则新建一个锁映射，然后添加一个锁
        if (!pageMap.containsKey(pid)) {
            ConcurrentHashMap<TransactionId, Lock> lockMap = new ConcurrentHashMap<TransactionId, Lock>();
            lockMap.put(tid, new Lock(tid, isShared));
            pageMap.put(pid, lockMap);
            return true;
        }

        //有锁时，获取锁映射
        ConcurrentHashMap<TransactionId, Lock> lockMap = pageMap.get(pid);

        //当没有锁时，直接插入并返回
        if (lockMap.size() == 0) {
            lockMap.put(tid, new Lock(tid, isShared));
            return true;
        }

        //当该页只有一个锁时
        if (lockMap.size() == 1) {
            Lock lock = lockMap.entrySet().iterator().next().getValue();
            if (!lock.isShared) {//当唯一元素为写锁时，tid相同返回true，不同返回false
                return lock.tid == tid;
            } else if (lock.tid == tid) {///当唯一元素为读锁且tid相等时，直接更改锁的属性并返回
                lock.isShared = isShared;
                return true;
            }
        }

        //此时如果申请的是写锁，则直接返回
        if (!isShared)
            return false;

        //此时页面包含两个以上的读锁
        //没有锁则新建，然后返回
        if (!lockMap.containsKey(tid)) {
            lockMap.put(tid, new Lock(tid, isShared));
        }

        return true;
    }

    public synchronized boolean releaseLock(PageId pid, TransactionId tid) {

        //页面没有锁时返回false
        if (pageMap.get(pid) == null) {
            return false;
        }

        //页面有锁时，获取锁映射
        ConcurrentHashMap<TransactionId, Lock> lockMap = pageMap.get(pid);

        //事务有锁时，删去
        if (lockMap.containsKey(tid)) {
            lockMap.remove(tid);
            return true;
        }

        return false;
    }

    public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
        //根据当前有无页面锁以及事务锁判断
        return pageMap.containsKey(pid) && pageMap.get(pid).containsKey(tid);
    }

    public class Lock {
        public TransactionId tid;//得到该锁的事务ID
        public boolean isShared;//是否为共享锁

        public Lock(TransactionId tid, boolean isShared) {
            this.tid = tid;
            this.isShared = isShared;
        }
    }
}