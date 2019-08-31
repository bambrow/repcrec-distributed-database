import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class holds the information for a
 * single transaction, and acts as the holder
 * for all operations under its id.
 *
 * @author Yichang Chen
 * Updated: 12/07/2018
 */
public class Transaction {

    public enum TransactionType {
        READ_WRITE,
        READ_ONLY,
    }

    private final int id; // transaction id
    private final TransactionType type; // transaction type
    private final long birthTime; // birth time
    private boolean isEnd; // indicator for ended status
    private boolean isAborted; // indicator for aborted status
    private boolean isCommittable; // indicator for commitable status
    private int pendingOperationCount; // pending operation count
    private List<Operation> operationList; // operation list

    public Transaction(int id, TransactionType type, long birthTime) {
        this.id = id;
        this.birthTime = birthTime;
        this.type = type;
        this.isEnd = false;
        this.isCommittable = false;
        this.isAborted = false;
        this.pendingOperationCount = 0;
        this.operationList = new ArrayList<>();
    }

    /**
     * Increment pending operation count.
     */
    public void increasePendingOperationCount() {
        this.pendingOperationCount += 1;
    }

    /**
     * Decrement pending operation count.
     */
    public void decreasePendingOperationCount() {
        this.pendingOperationCount -= 1;
    }

    /**
     * Check if it is aborted.
     * @return true if aborted
     */
    public boolean isAborted() {
        return isAborted;
    }

    /**
     * Abort the transaction.
     */
    public void setToAborted() {
        this.isAborted = true;
    }

    /**
     * Finish the transaction.
     */
    public void setFinished() {
        isEnd = true;
    }

    /**
     * Check if it can be commited.
     * @return true if commitable
     */
    public boolean isCommittable() {
        if (pendingOperationCount == 0 && isEnd && !isAborted()) {
            this.isCommittable = true;
        }
        return isCommittable;
    }

    /**
     * Get transaction id.
     * @return transaction id
     */
    public int getId() {
        return id;
    }

    /**
     * Get transaction type.
     * @return transaction type
     */
    public TransactionType getType() {
        return type;
    }

    /**
     * Get birth time.
     * @return birth time
     */
    public long getBirthTime() {
        return birthTime;
    }

    /**
     * Get operation list.
     * @return operation list
     */
    public List<Operation> getOperationList() {
        return operationList;
    }

    /**
     * Add an operation to operation list.
     * @param operation
     */
    public void addOperation(Operation operation) {
        this.operationList.add(operation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
