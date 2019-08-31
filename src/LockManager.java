import java.util.ArrayList;
import java.util.List;

/**
 * This class serves as a lock manager for
 * a single variable in a specific site.
 *
 * @author Yichang Chen, Weiqiang Li
 * Updated: 12/07/2018
 */
public class LockManager {

    private final int siteId; // site id
    private final int variableId; // variable id
    private List<Lock> lockList; // the list of lock that holds

    public LockManager(int siteId, int variableId) {
        this.siteId = siteId;
        this.variableId = variableId;
        this.lockList = new ArrayList<>();
    }

    /**
     * Get site id.
     * @return site id
     */
    public int getSiteId() {
        return siteId;
    }

    /**
     * Get variable id.
     * @return variable id
     */
    public int getVariableId() {
        return variableId;
    }

    /**
     * Check if a read lock can be obtained.
     *
     * @param operation - operation asking for read lock
     * @return true if read lock can be issued
     */
    public boolean canGetReadLock(Operation operation) {
        int transactionId = operation.getTransactionId();
        for (Lock lock : lockList) {
            if (lock.getTransactionId() == transactionId) {
                return true;
            }
            if (lock.getType() == Lock.LockType.WRITE_LOCK) {
                return false;
            }
        }
        return true;
    }


    /**
     * Check if a write lock can be obtained.
     * @param operation - operation asking for write lock
     * @return true if read lock can be issued
     */
    public boolean canGetWriteLock(Operation operation) {
        int transactionId = operation.getTransactionId();
        if (lockList.isEmpty()) {
            return true;
        } else {
            return lockList.size() == 1 && lockList.get(0).getTransactionId() == transactionId;
        }
    }

    /**
     * Clear lock list and opearation list. Called when site fails.
     */
    public void clear() {
        lockList.clear();
//        operationWaitList.clear();
    }

    /**
     * Release locks for a single transaction. Called when transaction is committed or aborted.
     * @param transactionId
     */
    public void releaseLockByTransactionId(int transactionId) {
        List<Lock> newLockList = new ArrayList<>();
        for (Lock lock : lockList) {
            if (lock.getTransactionId() != transactionId) {
                newLockList.add(lock);
            }
        }
        lockList = newLockList;
    }

    /**
     * Find the lock in the lock list given a transaction.
     * @param transactionId
     * @return the lock the transaction holds
     */
    private Lock findLock(int transactionId) {
        for (Lock lock : lockList) {
            if (lock.getTransactionId() == transactionId) {
                return lock;
            }
        }
        return null;
    }

    /**
     * Tests if the variable is blocked by other transactions.
     * @param transactionId
     * @return true if it is blocked
     */
    public boolean isLockedByOtherTransaction(int transactionId) {
        if (lockList.size() == 0) {
            return false;
        }
        if (lockList.size() == 1
                && (lockList.size() == 1 && lockList.get(0).getTransactionId() == transactionId)) {
            return false;
        }
        return true;
    }

    /**
     * Acquire the lock by operation.
     * @param operation
     * @return the lock acquired
     */
    public Lock acquireLock(Operation operation) {
        if (operation.getType() == Operation.OperationType.READ) {
            return acquireReadLock(operation);
        } else {
            return acquireWriteLock(operation);
        }
    }

    /**
     * Acquire read lock by operation.
     * @param operation
     * @return the lock acquired
     */
    private Lock acquireReadLock(Operation operation) {
        Lock lock = findLock(operation.getTransactionId());
        if (lock == null) {
            lock = new Lock(operation.getTransactionId(), operation.getVariableId(), Lock.LockType.READ_LOCK);
            lockList.add(lock);
        }
        return lock;
    }

    /**
     * Acquire write lock by operation.
     * @param operation
     * @return the write lock acquired
     */
    private Lock acquireWriteLock(Operation operation) {
        Lock lock = findLock(operation.getTransactionId());
        if (lock == null) {
            lock = new Lock(operation.getTransactionId(), operation.getVariableId(), Lock.LockType.WRITE_LOCK);
            lockList.add(lock);
        } else {
            lock.upgradeToWriteLock();
        }
        return lock;
    }

}
