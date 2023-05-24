package simpledb;

import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    //将页面ID、事务ID映射到对应的锁,ConcurrentHashMap线程安全
    ConcurrentHashMap<PageId, ArrayList<Lock>> lockMap;
    //储存等待页面的信息
    private ConcurrentHashMap<TransactionId, PageId> waitingInfo;

    //构造函数，创建映射
    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
        waitingInfo = new ConcurrentHashMap<>();
    }

    public synchronized boolean getRWLock(TransactionId tid, PageId pid, boolean isShared){
        return isShared?getRLock(tid,pid):getWLock(tid,pid);
    }

    public synchronized boolean getRLock(TransactionId tid, PageId pid) {
        ArrayList<Lock> list = lockMap.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {//pid上只有一个锁
                Lock lock = list.iterator().next();
                if (lock.tid.equals(tid)) {//判断是否为自己的锁
                    //如果是读锁，直接返回，否则加锁再返回
                    return lock.isShared || lock(tid, pid, true);
                } else {
                    //如果是别人的读锁，加锁再返回，是写锁则需要等待
                    return lock.isShared ? lock(tid, pid, true) : wait(tid, pid);
                }
            } else {
                for (Lock lock : list) {
                    if (!lock.isShared) {
                        //如果其中有一个写锁，那么根据是否为自己的来判断是否等待
                        return lock.tid.equals(tid) || wait(tid, pid);
                    } else if (lock.tid.equals(tid)) {
                        //如果是读锁且是tid的，直接返回
                        return true;
                    }
                }
                //如果都是读锁且没有tid的
                return lock(tid, pid, true);
            }
        } else {
            return lock(tid, pid, true);
        }
    }
    
    public synchronized boolean getWLock(TransactionId tid, PageId pid) {
        ArrayList<Lock> list = lockMap.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {//如果pid上只有一个锁
                Lock lock = list.iterator().next();
                //如果是自己的写锁，直接返回；如果是自己的读锁，加锁再返回
                //如果这个锁是别人的，进行等待
                return lock.tid.equals(tid) ? !lock.isShared || lock(tid, pid, false) : wait(tid, pid);
            } else {
                if (list.size() == 2) {
                    for (Lock lock : list) {
                        if (lock.tid.equals(tid) && !lock.isShared) {
                            return true;//两个锁而且有一个自己的写锁
                        }
                    }
                }
                return wait(tid, pid);
            }
        } else {
            //pid上没有锁，可以加写锁
            return lock(tid, pid, false);
        }
    }

    public synchronized Lock getLock(TransactionId tid, PageId pid) {
        ArrayList<Lock> list = lockMap.get(pid);
        if (list == null) return null;
        for (Lock lock : list) {
            if (lock.tid.equals(tid)) {//找到了对应的锁
                return lock;
            }
        }
        return null;
    }

    public synchronized boolean unLock(TransactionId tid, PageId pid) {
        ArrayList<Lock> list = lockMap.get(pid);
        if (list == null) return false;
        Lock lock = getLock(tid, pid);
        if (lock == null) return false;
        list.remove(lock);
        return true;
    }

    //上锁
    private synchronized boolean lock(TransactionId tid, PageId pid, boolean isShared) {
        if (!lockMap.containsKey(pid)) {//如果该页面的锁映射还不存在，则先创建锁映射
            lockMap.put(pid, new ArrayList<>());
        }

        ArrayList<Lock> list=lockMap.get(pid);
        list.add(new Lock(tid, isShared));//将锁加入锁列表
        waitingInfo.remove(tid);//tid已经得到锁，故可删去
        return true;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        //根据当前有无页面锁以及事务锁判断
        return getLock(tid,pid)!=null;
    }

    public synchronized void transactionComplete(TransactionId tid){
        List<PageId> toRelease = getAllLocksByTid(tid);
        for (PageId pid : toRelease) {
            unLock(tid, pid);
        }
    }

    public class Lock {
        public TransactionId tid;//得到该锁的事务ID
        public boolean isShared;//是否为共享锁

        public Lock(TransactionId tid, boolean isShared) {
            this.tid = tid;
            this.isShared = isShared;
        }
    }

    //只是处理好waitingInfo的信息然后返回false
    private synchronized boolean wait(TransactionId tid, PageId pid) {
        waitingInfo.put(tid, pid);
        return false;
    }

    private synchronized ArrayList<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> ans = new ArrayList<>();
        for (Map.Entry<PageId, ArrayList<Lock>> entry : lockMap.entrySet()) {
            for (Lock lock : entry.getValue()) {
                if (lock.tid.equals(tid)) {
                    ans.add(entry.getKey());
                }
            }
        }
        return ans;
    }

    //本事务tid需要检测“正在等待的资源的拥有者是否已经直接或间接的在等待本事务tid已经拥有的资源
    public synchronized boolean deadlockOccurred(TransactionId tid, PageId pid) {
        ArrayList<Lock> holders = lockMap.get(pid);//具有当前页面锁的锁列表
        if (holders == null || holders.size() == 0)return false;//此时显然不会产生死锁
        ArrayList<PageId> pids = getAllLocksByTid(tid);//当前事务持有的锁
        for (Lock lock : holders) {
            TransactionId holderTid = lock.tid;
            //去掉T1，因为虽然上图没画出这种情况，但T1可能同时也在其他Page上有读锁，这会影响判断结果
            if (!holderTid.equals(tid)) {
                //判断holderTid是否直接或间接在等待pids中的某元素
                if (isWaitingResources(holderTid, pids, tid)) {
                    return true;
                }
            }
        }
        return false;
    }

    //判断tid是否直接或间接地在等待pids中的某个资源
    private synchronized boolean isWaitingResources(TransactionId tid, ArrayList<PageId> pids, TransactionId toRemove) {
        PageId waitingPage = waitingInfo.get(tid);//得到当前tid在等待的page
        if (waitingPage == null) {//没有等待列表时返回false
            return false;
        }

        for (PageId pid : pids) {
            if (pid.equals(waitingPage)) {//若等待pids中的任意一个返回true
                return true;
            }
        }

        //到达这里说明tid并不直接在等待pids中的任意一个，但有可能间接在等待
        //通过递归的方式得到间接等待
        ArrayList<Lock> holders = lockMap.get(waitingPage);
        if (holders == null || holders.size() == 0) return false;//该资源没有拥有者
        for (Lock lock : holders) {
            TransactionId holderTid = lock.tid;
            if (!holderTid.equals(toRemove)) {//去掉toRemove，在toRemove刚好拥有waitingResource的读锁时就需要
                boolean isWaiting = isWaitingResources(holderTid, pids, toRemove);
                if (isWaiting) return true;
            }
        }

        //此时说明每一个holder都不直接或间接等待pids，故tid也非间接等待pids
        return false;
    }
}