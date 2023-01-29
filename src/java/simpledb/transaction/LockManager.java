package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

        public void setTransactionId(TransactionId transactionId) {
            this.transactionId = transactionId;
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

    private Map<PageId, List<SimpleDBLock>> lockCache;

    public LockManager() {
        this.lockCache = new ConcurrentHashMap<>();
    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions) {
        if (tid == null) {
            return true;
        }
        SimpleDBLock lock = new SimpleDBLock(tid, permissions);
        List<SimpleDBLock> locks = lockCache.get(pid);
        if (locks == null || locks.size() == 0) {
            locks = locks == null ? new ArrayList<>() : locks;
            locks.add(lock);
            lockCache.put(pid, locks);
            return true;
        }

        if (locks.size() == 1) {
            SimpleDBLock curLock = locks.get(0);
            if (curLock.getTransactionId().equals(tid)) {
                if (curLock.getPermissions().equals(Permissions.READ_ONLY)
                        && lock.getPermissions().equals(Permissions.READ_WRITE)) {
                    curLock.setPermissions(Permissions.READ_WRITE);
                }
                return true;
            } else {
                if (curLock.getPermissions().equals(Permissions.READ_ONLY)
                        && lock.getPermissions().equals(Permissions.READ_ONLY)) {
                    locks.add(lock);
                    return true;
                }
                return false;
            }
        }
        if (lock.getPermissions().equals(Permissions.READ_WRITE)) {
            return false;
        }
        for (SimpleDBLock l: locks) {
            if (l.getTransactionId().equals(tid)) {
                return true;
            }
        }
        locks.add(lock);
        return true;
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        List<SimpleDBLock> locks = lockCache.get(pid);
        for (SimpleDBLock l: locks) {
            if (l.getTransactionId().equals(tid)) {
                locks.remove(l);
                if (locks.size() == 0) {
                    lockCache.remove(pid);
                }
                break;
            }
        }
    }

    public synchronized void releaseAllLock(TransactionId tid) {
        for (Map.Entry<PageId, List<SimpleDBLock>> entry: lockCache.entrySet()) {
            List<SimpleDBLock> locks = entry.getValue();
            for (SimpleDBLock l: locks) {
                if (l.getTransactionId().equals(tid)) {
                    locks.remove(l);
                    if (locks.size() == 0) {
                        lockCache.remove(entry.getKey());
                    }
                    break;
                }
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
}
