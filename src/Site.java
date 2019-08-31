import java.util.*;

public class Site {

    public enum SiteStatus {
        RUNNING,
        FAILED,
        RECOVERED,
    }

    private static final int VARIABLE_COUNT = 20;

    private final int id;
    private SiteStatus status;
    private boolean debugMode = false;
    private Map<Integer, Variable> variableMap; // <key: Variable id, val: Variable object>
    private Map<Integer, LockManager> lockManagerMap; // <key: Variable id, val: LockManager object>
    private Map<Integer, List<Operation>> transactionOperationMap; // <key: Transaction id, val: list of operations to be committed>
    // only the validated operations can be put in this map, it is guaranteed that it can be committed if site does not fail

    public Site(int id) {
        this.id = id;
        this.status = SiteStatus.RUNNING;
        this.variableMap = new HashMap<>();
        this.lockManagerMap = new HashMap<>();
        this.transactionOperationMap = new HashMap<>();
    }

    public void init() {
        for (int i = 1; i <= VARIABLE_COUNT; i++) {
            if (i % 2 == 0 || id == 1 + i % 10) {
                variableMap.put(i, new Variable(i));
                lockManagerMap.put(i, new LockManager(id, i));
            }
        }
    }

    public int getId() {
        return id;
    }

    public SiteStatus getStatus() {
        return status;
    }

    public void setStatus(SiteStatus siteStatus) {
        this.status = siteStatus;
    }

    /**
     * Dump all variables in the site
     */
    public void dump() {
        System.out.format("site %s – ", id);
        for (int i = 1; i <= VARIABLE_COUNT; i++) {
            if (i % 2 == 0 || id == 1 + i % 10) {
                if (debugMode && getVariableById(i) == i * 10) {
                    continue;
                }
                System.out.format("x%s: %s, ", i, getVariableById(i));
            }
        }
        System.out.println();
    }

    /**
     * Dump one variable in the site
     * @param i id of the variable to be dumped
     */
    public void dump(int i) {
        if (variableMap.containsKey(i)) {
            System.out.format("site %s – ", id);
            System.out.format("x%s: %s, ", i, getVariableById(i));
            System.out.println();
        }
    }

    public boolean isFailed() {
        return status == SiteStatus.FAILED;
    }

    public boolean isRecovered() {
        return status == SiteStatus.RECOVERED;
    }

    /**
     * Called when a site fails
     * @return transactions that need to be aborted
     */
    public List<Integer> fail() {
        status = SiteStatus.FAILED;
        for (int i : lockManagerMap.keySet()) {
            lockManagerMap.get(i).clear();
        }
        for (int i : variableMap.keySet()) {
            if (i % 2 == 0) {
                variableMap.get(i).fail();
            }
        }
        List<Integer> abortedTransactionList = new ArrayList<>(transactionOperationMap.keySet());
        transactionOperationMap.clear();
        return abortedTransactionList;
    }

    /**
     * Called when a site recovers
     */
    public void recover() {
        status = SiteStatus.RECOVERED;
    }

    public boolean containsVariable(int variableId) {
        return variableMap.containsKey(variableId);
    }

    /**
     * Put operation in the storage queue for the transaction
     * @param transactionId id of the transaction
     * @param operation operation to be put into the queue
     */
    private void putOperationInQueue(int transactionId, Operation operation) {
        if (!transactionOperationMap.containsKey(transactionId)) {
            transactionOperationMap.put(transactionId, new ArrayList<>());
        }
        transactionOperationMap.get(transactionId).add(operation);
    }

    private int getVariableById(int variableId) {
        return variableMap.get(variableId).getValue();
    }

    private void setVariableById(int variableId, int value, long updateTime) {
        variableMap.get(variableId).updateValue(value, updateTime);
    }

    /**
     * Check if the operation could perform a read-write read
     * @param operation operation to be checked
     * @return true if can read, false otherwise
     */
    public boolean canReadVariableRW(Operation operation) {
        int variableId = operation.getVariableId();
        int transactionId = operation.getTransactionId();
        if (isFailed()) {
            return false;
        }
        if (isRecovered()) {
            return variableMap.containsKey(variableId) && variableMap.get(variableId).isReadable();
        }
        return variableMap.containsKey(variableId) && lockManagerMap.get(variableId).canGetReadLock(operation);
    }

    /**
     * Check if the operation could perform a write
     * @param operation operation to be checked
     * @return true if can read, false otherwise
     */
    public boolean canWriteVariableRW(Operation operation) {
        if (getStatus() == SiteStatus.FAILED) {
            return false;
        }
        int variableId = operation.getVariableId();

        return variableMap.containsKey(operation.getVariableId())
                && lockManagerMap.get(operation.getVariableId()).canGetWriteLock(operation);
    }

    /**
     * Check if the operation would be the initial write after the site fails
     * @param operation operation to be checked
     * @return true if it's the initial write, false otherwise
     */
    public boolean initialWriteAfterRecover(Operation operation) {
        int variableId = operation.getVariableId();
        if (status == SiteStatus.RECOVERED
                && variableMap.containsKey(operation.getVariableId())
                && !variableMap.get(variableId).isReadable()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if the variable is currently being locked by some transaction
     * @param operation operation that issues the check
     * @return true if the variable is locked, false otherwise
     */
    public boolean variableBeingLocked(Operation operation) {
        if (getStatus() == SiteStatus.FAILED) {
            return false;
        }

        return getStatus() == SiteStatus.RUNNING
                && variableMap.containsKey(operation.getVariableId())
                && lockManagerMap.get(operation.getVariableId()).isLockedByOtherTransaction(operation.getTransactionId());
    }

    /**
     * Perform a read in a read-write transaction
     * @param operation operation that issues the read
     * @return value of the variable
     */
    public int readVariableRW(Operation operation) {
        LockManager lockManager = lockManagerMap.get(operation.getVariableId());

        lockManager.acquireLock(operation);
        int value = getVariableById(operation.getVariableId());
        operation.setReadValue(value);
        putOperationInQueue(operation.getTransactionId(), operation);

        return value;
    }

    /**
     * Perform a read in a read-write transaction
     * @param operation operation that issues the read
     * @return value of the variable
     */
    public void writeVariableRW(Operation operation) {
        LockManager lockManager = lockManagerMap.get(operation.getVariableId());
        lockManager.acquireLock(operation);
        putOperationInQueue(operation.getTransactionId(), operation);

    }

    /**
     * Check if the operation could perform a read-only read
     * @param operation operation to be checked
     * @return true if can read, false otherwise
     */
    public boolean canReadVariableRO(Operation operation) {
        int variableId = operation.getVariableId();
        int transactionId = operation.getTransactionId();
        if (isFailed()) {
            return false;
        }
        return variableMap.containsKey(variableId) && variableMap.get(variableId).isReadable();
    }

    /**
     * Perform a read in a read-only transaction
     * @param operation operation that issues the read
     * @return value of the variable
     */
    public int readVariableRO(Operation operation) {
        // for read-only transactions the read operation current time should set to transaction birth time
        // because it should read the value as if the value when the transaction was constructed
        Variable variable = variableMap.get(operation.getVariableId());
        int value = variableMap.get(operation.getVariableId()).getValueBeforeTime(operation.getCurrentTime());
        operation.setReadValue(value);
        putOperationInQueue(operation.getTransactionId(), operation);
        return value;
    }

    /**
     * Commit the transaction in the site
     * @param transaction transaction to be committed
     * @return true if commit succeeds, false otherwise
     */
    public boolean commitTransaction(Transaction transaction) {
        if (status == SiteStatus.FAILED) {
            return false;
        }
        int transactionId = transaction.getId();
        if (!transactionOperationMap.containsKey(transactionId)) {
            return true;
        }
        List<Operation> operations = transactionOperationMap.get(transactionId);

        for (Operation operation : operations) {
            int variableId = operation.getVariableId();
            int value = operation.getValue();
            if (operation.getType() == Operation.OperationType.READ) {
                if (transaction.getType() == Transaction.TransactionType.READ_WRITE) {
                    lockManagerMap.get(variableId).releaseLockByTransactionId(transactionId);
                }
            } else {
                setVariableById(variableId, value, operation.getCurrentTime());
                lockManagerMap.get(variableId).releaseLockByTransactionId(transactionId);
            }
        }

        transactionOperationMap.remove(transactionId);
        return true;
    }

    /**
     * Abort the transaction
     * @param transactionId id of the transaction to be aborted
     */
    public void abortTransaction(int transactionId) {
        for (int i : lockManagerMap.keySet()) {
            lockManagerMap.get(i).releaseLockByTransactionId(transactionId);
        }
        if (transactionOperationMap.containsKey(transactionId)) {
            transactionOperationMap.remove(transactionId);
        }
    }
}
