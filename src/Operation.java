import java.util.Objects;

/**
 * This class holds the information for a single
 * operation for a transaction.
 *
 * @author Yichang Chen, Weiqiang Li
 * Updated: 12/06/2018
 */
public class Operation {

    public enum OperationType {
        READ,
        WRITE
    }

    private final int transactionId; // transaction id
    private final OperationType type; // operation type
    private final int variableId; // variable id
    private int value; // the value to write; can be used to store read data in read operations
    private final long currentTime; // operation time
    private final Transaction.TransactionType transactionType; // transaction type of this operation

    public Operation(int transactionId,
                     OperationType type,
                     int variableId,
                     int value,
                     long currentTime,
                     Transaction.TransactionType transactionType) {
        this.transactionId = transactionId;
        this.type = type;
        this.variableId = variableId;
        this.value = type == OperationType.READ ? Integer.MIN_VALUE : value;
        this.currentTime = currentTime;
        this.transactionType = transactionType;

    }

    /**
     * Get transaction id.
     * @return transaction id
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * Get operation type.
     * @return operation type
     */
    public OperationType getType() {
        return type;
    }

    /**
     * Get variable id.
     * @return variable id
     */
    public int getVariableId() {
        return variableId;
    }

    /**
     * Get the read/write value.
     * @return value
     */
    public int getValue() {
        return value;
    }

    /**
     * Get operation time.
     * @return operation time
     */
    public long getCurrentTime() {
        return currentTime;
    }

    /**
     * Get transaction type.
     * @return transaction type
     */
    public Transaction.TransactionType getTransactionType() {
        return transactionType;
    }

    /**
     * Set the value to the read value.
     * @param value - the read value
     */
    public void setReadValue(int value) {
        if (this.type == OperationType.READ) {
            this.value = value;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return transactionId == operation.transactionId &&
                variableId == operation.variableId &&
                value == operation.value &&
                currentTime == operation.currentTime &&
                type == operation.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, type, variableId, value, currentTime);
    }
}