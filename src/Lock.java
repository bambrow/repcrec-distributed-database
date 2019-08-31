import java.util.Objects;

/**
 * This class holds the information for a single lock
 * associated with a transaction and a variable.
 *
 * @author Weiqiang Li, Yichang Chen
 * Updated: 12/06/2018
 */
public class Lock {

    public enum LockType {
        READ_LOCK,
        WRITE_LOCK
    }

    private final int transactionId; // transaction id
    private final int variableId; // variable id
    private LockType type; // lock type

    public Lock(int transactionId, int variableId, LockType type) {
        this.transactionId = transactionId;
        this.variableId = variableId;
        this.type = type;
    }

    /**
     * Get transaction id.
     * @return transaction id
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * Get variable id.
     * @return variable id
     */
    public int getVariableId() {
        return variableId;
    }

    /**
     * Get lock type.
     * @return lock type
     */
    public LockType getType() {
        return type;
    }

    /**
     * Upgrade this lock to write lock.
     */
    public void upgradeToWriteLock() {
        type = LockType.WRITE_LOCK;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lock lock = (Lock) o;
        return transactionId == lock.transactionId &&
                variableId == lock.variableId &&
                type == lock.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, variableId, type);
    }

}
