package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public class SimpleDBLock {
        private TransactionId transactionId;
        private Permissions permissions;

        public SimpleDBLock(TransactionId transactionId, Permissions permissions) {
            this.transactionId = transactionId;
            this.permissions = permissions;
        }

        public TransactionId getTransactionId() {
            return transactionId;
        }

        public Permissions getPermissions() {
            return permissions;
        }

        public void setPermissions(Permissions permissions) {
            this.permissions = permissions;
        }

        @Override
        public String toString() {
            return new StringBuilder().append("Lock{permissions=").append(permissions.toString())
                    .append(", transactionId=").append(transactionId.toString()).append("}").toString();
        }
    }

    private Map<PageId, Set<SimpleDBLock>> lockCache;

    public LockManager() {
        this.lockCache = new ConcurrentHashMap<>();
    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions) {
        if (tid == null) {
            return true;
        }
        SimpleDBLock lock = new SimpleDBLock(tid, permissions);
        Set<SimpleDBLock> locks = lockCache.get(pid);

        if (locks == null) {
            locks = new HashSet<>();
            locks.add(lock);
            lockCache.put(pid, locks);
            return true;
        } else if (locks.isEmpty()) {
            locks.add(lock);
            return true;
        }

        if (lock.getPermissions().equals(Permissions.READ_ONLY)) {
            for (SimpleDBLock cur: locks) {
                if (cur.getTransactionId().equals(tid)) {
                    return true;
                } else if (cur.getPermissions().equals(Permissions.READ_WRITE)) {
                    return false;
                }
            }
            locks.add(lock);
            return true;
        } else {
            SimpleDBLock first = locks.iterator().next();
            if (locks.size() == 1 && first.getTransactionId().equals(tid)) {
                if (first.getPermissions().equals(Permissions.READ_ONLY)) {
                    locks = new HashSet<>();
                    locks.add(lock);
                    lockCache.put(pid, locks);
                    return true;
                } else {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        Set<SimpleDBLock> locks = lockCache.get(pid);
        if (locks.contains(tid)) {
            locks.remove(tid);
            if (locks.isEmpty()) {
                lockCache.remove(pid);
            }
        }
    }

    public synchronized void releaseAllLock(TransactionId tid) {
        for (Map.Entry<PageId, Set<SimpleDBLock>> entry: lockCache.entrySet()) {
            Set<SimpleDBLock> locks = entry.getValue();
            List<SimpleDBLock> toBeRemoved = new ArrayList<>();
            for (SimpleDBLock lock: locks) {
                if (lock.getTransactionId().equals(tid)) {
                    toBeRemoved.add(lock);
                }
            }
            for (SimpleDBLock r : toBeRemoved) {
                locks.remove(r);
            }
        }
    }

    public synchronized Boolean holdsLock(TransactionId tid, PageId pid) {
        for (SimpleDBLock l: lockCache.get(pid)) {
            if (l.getTransactionId().equals(tid)) {
                return true;
            }
        }
        return false;
    }

    public synchronized Set<TransactionId> peekLock(PageId pageId) {
        Set<SimpleDBLock> locks = lockCache.get(pageId);
        Set<TransactionId> res = new HashSet<>();
        if (locks == null) {
            return res;
        } else {
            for (SimpleDBLock lock: locks) {
                res.add(lock.getTransactionId());
            }
            return res;
        }
    }
}
