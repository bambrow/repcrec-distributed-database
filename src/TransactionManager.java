import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class TransactionManager {

    private static final String WRITE_PREFIX = "W";
    private static final String READ_PREFIX = "R";
    private static final String BEGIN_PREFIX = "begin";
    private static final String BEGIN_RO_PREFIX = "beginRO";
    private static final String END_PREFIX = "end";
    private static final String DUMP_PREFIX = "dump";
    private static final String FAIL_PREFIX = "fail";
    private static final String RECOVER_PREFIX = "recover";

    private static final int VARIABLE_COUNT = 20;
    private static final int SITE_COUNT = 10;

    private boolean debugMode = false;

    private File file;
    private List<Site> siteList;
    private long currenttime = 1;
    private Map<Integer, Transaction> transactionMap;
    private List<Operation> operationWaitlist;
    private DeadlockManager deadlockManager;
    private Set<Integer> abortedTransactionSet;
    private Map<Integer, List<Integer>> variableVisitedTransactionMap;
    private Map<Integer, List<Operation>> variableWaitlistMap;
    // <key: Variable id, value: List of trasaction id that visited(RW) this variable>

    public TransactionManager() {
        siteList = new ArrayList<>();
        transactionMap = new HashMap<>();
        operationWaitlist = new ArrayList<>();
        deadlockManager = new DeadlockManager();
        abortedTransactionSet = new HashSet<>();
        variableVisitedTransactionMap = new HashMap<>();
        variableWaitlistMap = new HashMap<>();

        for (int i = 1; i <= SITE_COUNT; i++) {
            Site site = new Site(i);
            site.init();
            siteList.add(site);
        }
        for (int i = 1; i <= VARIABLE_COUNT; i++) {
            variableVisitedTransactionMap.put(i, new ArrayList<>());
            variableWaitlistMap.put(i, new ArrayList<>());
        }

    }

    /**
     * Run with file input
     * @param inputFilePath
     */
    public void startFileMode(String inputFilePath) {
        System.out.println();
        System.out.println();
        try {
            BufferedReader reader;
            file = new File(inputFilePath);
            reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                System.out.println("Your input: " + line);
                parseLine(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (debugMode) {
            System.out.println();
            System.out.println("Dumping modified values");
            for (Site site : siteList) {
                site.dump();
            }
        }
    }

    /**
     * Run with standard input
     * @param reader
     */
    public void startCommandLineMode(BufferedReader reader) {
        System.out.println();
        System.out.println();
        try {
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                System.out.println("Input: " + line);
                parseLine(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse input line by line and execute
     * @param line line read from standard input or file input
     */
    private void parseLine(String line) {
        line = line.trim();
        currenttime += 1;
        if (line.startsWith(BEGIN_RO_PREFIX)) {
            handleBeginReadOnlyTransaction(line);
        } else if (line.startsWith(BEGIN_PREFIX)) {
            handleBeginTransaction(line);
        } else if (line.startsWith(END_PREFIX)) {
            handleEndTransaction(line);
        } else if (line.startsWith(DUMP_PREFIX)) {
            handleDump(line);
        } else if (line.startsWith(READ_PREFIX)) {
            String info = line.substring(2, line.length() - 1).replace(" ", "");
            String[] infos = info.split(",");
            int transactionId = Integer.parseInt(infos[0].replaceAll("\\D+", ""));
            int variableId = Integer.parseInt(infos[1].replaceAll("\\D+", ""));

            Transaction transaction = getTransactionById(transactionId);
            transaction.increasePendingOperationCount();
            Operation operation;


            if (transaction.getType() == Transaction.TransactionType.READ_ONLY) {
                operation = new Operation(transactionId,
                        Operation.OperationType.READ,
                        variableId,
                        -1,
                        transaction.getBirthTime(),
                        transaction.getType());
                handleReadRO(transaction, operation, false);
            } else {
                operation = new Operation(transactionId,
                        Operation.OperationType.READ,
                        variableId,
                        -1,
                        getCurrentTime(),
                        transaction.getType());
                handleReadRW(transaction, operation, false);
            }

        } else if (line.startsWith(WRITE_PREFIX)) {
            String info = line.substring(2, line.length() - 1).replace(" ", "");
            String[] infos = info.split(",");
            int transactionId = Integer.parseInt(infos[0].replaceAll("\\D+", ""));
            int variableId = Integer.parseInt(infos[1].replaceAll("\\D+", ""));
            int newValue = Integer.parseInt(infos[2]);

            Transaction transaction = getTransactionById(transactionId);
            transaction.increasePendingOperationCount();
            Operation operation = new Operation(transactionId,
                    Operation.OperationType.WRITE,
                    variableId,
                    newValue,
                    getCurrentTime(),
                    transaction.getType());

            handleWriteOperation(operation, transaction, false);
        } else if (line.startsWith(FAIL_PREFIX)) {
            handleSiteFail(line);
        } else if (line.startsWith(RECOVER_PREFIX)) {
            handleSiteRecover(line);
        } else {
            if (line.isEmpty()) {
                return;
            }
            System.out.println("Operation not recognized: " + line);
        }
    }

    /**
     * Handle begin transaction command
     * @param line
     */
    private void handleBeginTransaction(String line) {
        int transactionId = Integer.parseInt(line.replaceAll("\\D+", ""));
        deadlockManager.addVertex(transactionId);

        Transaction transaction = new Transaction(transactionId, Transaction.TransactionType.READ_WRITE, getCurrentTime());
        transactionMap.put(transactionId, transaction);
    }

    /**
     * Handle start read only transaction command
     * @param line
     */
    private void handleBeginReadOnlyTransaction(String line) {
        int transactionId = Integer.parseInt(line.replaceAll("\\D+", ""));
        Transaction transaction = new Transaction(transactionId, Transaction.TransactionType.READ_ONLY, getCurrentTime());
        transactionMap.put(transactionId, transaction);
    }

    /**
     * Handle end transaction command
     * @param line
     */
    private void handleEndTransaction(String line) {
        int transactionId = Integer.parseInt(line.replaceAll("\\D+", ""));
        Transaction transaction = getTransactionById(transactionId);
        transaction.setFinished();
        if (attemptCommitTransaction(transactionId)) {
            System.out.format("T%s commits", transactionId);
            System.out.println();
        }
        if (transaction.isAborted()) {
            System.out.format("T%s aborts", transactionId);
            System.out.println();
        }
    }

    /**
     * Handle read operation command
     * @param operation operation to be handled
     * @param transaction transaction that issues the operation
     * @param fromWaitlist if the method is called from the waitlist
     * @return true if read succeed, false otherwise
     */
    private boolean handleReadOperation(Operation operation, Transaction transaction, boolean fromWaitlist) {
        if (transaction.isAborted()) {
            return false;
        }

        int transactionId = transaction.getId();
        variableVisitedTransactionMap.get(operation.getVariableId()).add(transactionId);

        if (transaction.getType() == Transaction.TransactionType.READ_WRITE) {
            return handleReadRW(transaction, operation, fromWaitlist);
        } else {
            return handleReadRO(transaction, operation, fromWaitlist);
        }
    }

    /**
     * Handle a read write read operation
     * @param operation operation to be handled
     * @param transaction transaction that issues the operation
     * @param fromWaitlist if the method is called from the waitlist
     * @return true if read succeed, false otherwise
     */
    private boolean handleReadRW(Transaction transaction, Operation operation, boolean fromWaitlist) {
        int val = -1;
        boolean readSucceed = false;
        if (transaction.isAborted()) {
            return false;
        }

        if (!fromWaitlist) {
            addTransactionFromDeadlockManager(operation, transaction);
            addTransactionFromVisitedMap(operation, transaction);
        }

        for (int i = 1; i <= SITE_COUNT; i++) {
            Site site = getSiteById(i);
            if (site.canReadVariableRW(operation)) {
                val = site.readVariableRW(operation);
                readSucceed = true;
            }
        }

        if (readSucceed) {
            transaction.decreasePendingOperationCount();
            System.out.println("x" + operation.getVariableId() + ": " + val);
            return true;
        } else {
            operationWaitlist.add(operation);
            if (!fromWaitlist) {
                addOperationToWaitlistTail(operation);
            }
            deteckDeadlockAndAbortTransaction();
            return false;
        }
    }

    /**
     * Handle a read only read operation
     * @param operation operation to be handled
     * @param transaction transaction that issues the operation
     * @param fromWaitlist if the method is called from the waitlist
     * @return true if read succeed, false otherwise
     */
    private boolean handleReadRO(Transaction transaction, Operation operation, boolean fromWaitlist) {
        int val;
        if (transaction.isAborted()) {
            return false;
        }

        for (int i = 1; i <= SITE_COUNT; i++) {
            Site site = getSiteById(i);
            if (site.canReadVariableRO(operation)) {
                val = site.readVariableRO(operation);
                System.out.println("x" + operation.getVariableId() + ": " + val);
                transaction.decreasePendingOperationCount();
                return true;
            }
        }

        operationWaitlist.add(operation);
        return false;
    }

    /**
     * Handle a write command
     * @param operation operation to be handled
     * @param transaction transaction that issues the operation
     * @param fromWaitlist if the method is called from the waitlist
     * @return true if write succeed, false otherwise
     */
    private boolean handleWriteOperation(Operation operation, Transaction transaction, boolean fromWaitlist) {
        if (transaction.isAborted()) {
            return false;
        }

        if (!fromWaitlist) {
            addTransactionFromDeadlockManager(operation, transaction);
            addTransactionFromVisitedMap(operation, transaction);
        }

        boolean variableWritten = false;
        boolean beingLocked = false;

        for (Site site : siteList) {
            if (site.variableBeingLocked(operation)) {
                beingLocked = true;
                break;
            }
        }

        if (!beingLocked) {
            for (int i = 1; i <= SITE_COUNT; i++) {
                Site site = getSiteById(i);
                if (site.canWriteVariableRW(operation)
                        && (site.initialWriteAfterRecover(operation)
                            || fromWaitlist
                            || isNoOperationInWaitlist(operation.getVariableId()))) {
                    site.writeVariableRW(operation);
                    variableWritten = true;
                }
            }
        }

        if (variableWritten) {
            transaction.decreasePendingOperationCount();
            transaction.addOperation(operation);
            return true;
        } else {
            operationWaitlist.add(operation);
            if (!fromWaitlist) {
                addOperationToWaitlistTail(operation);
            }
            deteckDeadlockAndAbortTransaction();
            return false;
        }
    }

    /**
     * Handle a site fails command
     * @param line input command
     */
    private void handleSiteFail(String line) {
        int id = Integer.parseInt(line.replaceAll("\\D+", ""));
        Site site = getSiteById(id);
        List<Integer> visitedTransactionSet = site.fail();
        for (int transactionId : visitedTransactionSet) {
            abortTransaction(transactionId);
        }
    }

    /**
     * Handle site recover command
     * @param line input command
     */
    private void handleSiteRecover(String line) {
        int id = Integer.parseInt(line.replaceAll("\\D+", ""));
        Site site = getSiteById(id);
        site.recover();
        for (int transactionId : abortedTransactionSet) {
            abortTransaction(transactionId);
        }
    }

    /**
     * Handle dump/print command
     * @param line input command
     */
    private void handleDump(String line) {
        if (line.equals("dump()")) {
            for (Site site : siteList) {
                site.dump();
            }
        } else if (line.contains("x")) {
            int variableId = Integer.parseInt(line.replaceAll("\\D+", ""));
            for (Site site : siteList) {
                site.dump(variableId);
            }
        } else { // Dumping single site
            int siteId = Integer.parseInt(line.replaceAll("\\D+", ""));
            Site site = getSiteById(siteId);
            site.dump();
        }
    }

    /**
     * Get site object by it's id
     * @return the site object
     */
    private Site getSiteById(int siteId) {
        if (siteId <= 0 || siteId > SITE_COUNT) {
            System.out.println("Cannot get site with id: " + siteId);
            return null;
        } else {
            return siteList.get(siteId - 1);
        }
    }

    private Transaction getTransactionById(int transactionId) {
        return transactionMap.get(transactionId);
    }

    private List<Integer> getSitesByVariableId(int variableId) {
        List<Integer> list = new ArrayList<>();
        if (variableId > 0 && variableId <= VARIABLE_COUNT) {
            for (Site site : siteList) {
                if (site.containsVariable(variableId)) {
                    list.add(site.getId());
                }
            }
        }
        return list;
    }

    /**
     * @return current time
     */
    private long getCurrentTime() {
        return currenttime;
    }

    /**
     * Attempt to commit a transaction
     * @param transactionId transaction id of the transaction to be committed
     * @return true if commit succeeds, false otherwise
     */
    private boolean attemptCommitTransaction(int transactionId) {
        Transaction transaction = getTransactionById(transactionId);
        if (transaction.isCommittable()) {
            for (Site site : siteList) {
                site.commitTransaction(transaction);
            }
            runNextInWaitlist();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Abort the transaction
     * @param transactionId transaction id of the transaction to be aborted
     */
    private void abortTransaction(int transactionId) {
        Transaction transaction = getTransactionById(transactionId);
        abortedTransactionSet.add(transactionId);
        transaction.setToAborted();

        // Clean up in deadlock manager related
        removeTransactionFromDeadlockManager(transactionId);
        removeTransactionFromVisitedMap(transactionId);

        // Clear all operations in waitlist
        List<Operation> newWaitList = new ArrayList<>();
        for (Operation operation : operationWaitlist) {
            if (operation.getTransactionId() != transactionId) {
                newWaitList.add(operation);
            }
        }
        operationWaitlist = newWaitList;

        // Clear waitlist by each variable
        clearWaitlistByTransactionId(transactionId);

        // Clean up locks and pending operations in each site
        for (Site site : siteList) {
            if (site.getStatus() != Site.SiteStatus.FAILED) {
                site.abortTransaction(transactionId);
            }
        }
        runNextInWaitlist();
    }

    /**
     * Run next pending operation in the wait list
     */
    private void runNextInWaitlist() {
        if (operationWaitlist.isEmpty()) {
            return;
        }

        for (int i = 0; i < operationWaitlist.size(); i++) {
            Operation operation = operationWaitlist.remove(i);
            Transaction transaction = getTransactionById(operation.getTransactionId());

            if (transaction.getType() != Transaction.TransactionType.READ_ONLY) {
                removeOperaionFromWaitlistHead(operation.getVariableId());
            }

            boolean operationExecuted = false;
            if (operation.getType() == Operation.OperationType.READ) {
                operationExecuted = handleReadOperation(operation, getTransactionById(operation.getTransactionId()), true);
            } else {
                operationExecuted = handleWriteOperation(operation, getTransactionById(operation.getTransactionId()), true);
            }

            if (operationExecuted) {
                attemptCommitTransaction(transaction.getId());
                runNextInWaitlist();
            } else {
                operationWaitlist.remove(operationWaitlist.size() - 1);
                operationWaitlist.add(i, operation);
                if (transaction.getType() != Transaction.TransactionType.READ_ONLY) {
                    addOperationToWaitlistHead(operation);
                }
            }
        }
    }

    /**
     * Add transaction in the deadlock manager
     * @param operation operation to be executed
     * @param transaction transaction that the operation belongs to
     */
    private void addTransactionFromDeadlockManager(Operation operation, Transaction transaction) {
        int variableId = operation.getVariableId();
        int transactionId = transaction.getId();
        List<Integer> visitedTransactionList = variableVisitedTransactionMap.get(variableId);
        for (int id : visitedTransactionList) {
            deadlockManager.addChild(id, transactionId);
        }
    }

    /**
     * Add transaction in the visited list for each variable
     * @param operation operation to be executed
     * @param transaction transaction that the operation belongs to
     */
    private void addTransactionFromVisitedMap(Operation operation, Transaction transaction) {
        int transactionId = transaction.getId();
        int variableId = operation.getVariableId();
        variableVisitedTransactionMap.get(variableId).add(transactionId);
    }

    /**
     * Remove transaction in the dead lock manager
     * @param transactionId transaction id of the transaction to be removed
     */
    private void removeTransactionFromDeadlockManager(int transactionId) {
        deadlockManager.removeVertex(transactionId);
    }

    /**
     * Remove transaction in the visited list for each variable
     * @param transactionId transaction to be removed
     */
    private void removeTransactionFromVisitedMap(int transactionId) {
        for (int variableId : variableVisitedTransactionMap.keySet()) {
            List<Integer> visitedList = variableVisitedTransactionMap.get(variableId);
            List<Integer> newVisitedList = new ArrayList<>();
            for (int id : visitedList) {
                if (id != transactionId) {
                    newVisitedList.add(id);
                }
            }
            variableVisitedTransactionMap.put(variableId, newVisitedList);
        }
    }

    /**
     * Detect deadlock and abort the youngest transaction
     * @return true if deadlock found, false otherwise
     */
    private boolean deteckDeadlockAndAbortTransaction() {
        List<Integer> transactionIdList = deadlockManager.detectDeadlock();
        int targetId;
        if (transactionIdList.isEmpty()) {
            return false;
        } else {
            targetId = transactionIdList.get(0);
            for (int id : transactionIdList) {
                if (getTransactionById(id).getBirthTime() > getTransactionById(targetId).getBirthTime()) {
                    targetId = id;
                }
            }
            System.out.println("Deadlock detected, aborting: " + targetId);
            abortTransaction(targetId);
            return true;
        }
    }

    /**
     * Add operation to the tail of the waitlist
     * @param operation operation to be added
     */
    private void addOperationToWaitlistTail(Operation operation) {
        int variableId = operation.getVariableId();
        List<Operation> waitlist = variableWaitlistMap.get(variableId);
        if (!waitlist.contains(operation)) {
            waitlist.add(operation);
        }
    }

    /**
     * Check if there is any other operation waiting for the variable
     * @param variableId id of the variable to be checked
     * @return true if there is other operation waiting, false otherwise
     */
    private boolean isNoOperationInWaitlist(int variableId) {
        return variableWaitlistMap.get(variableId).isEmpty();
    }

    /**
     * Clean the wait list
     * @param transactionId operation belongs to this id needs to be cleaned
     */
    private void clearWaitlistByTransactionId(int transactionId) {
        for (int variableId : variableWaitlistMap.keySet()) {
            List<Operation> waitlist = variableWaitlistMap.get(variableId);
            List<Operation> newWaitlist = new ArrayList<>();
            for (Operation operation : waitlist) {
                if (operation.getTransactionId() != transactionId) {
                    newWaitlist.add(operation);
                }
            }
            variableWaitlistMap.put(variableId, newWaitlist);
        }
    }

    /**
     * Add operation to the head of the wait list
     * @param operation operation to be added
     */
    private void addOperationToWaitlistHead(Operation operation) {
        int variableId = operation.getVariableId();
        List<Operation> waitlist = variableWaitlistMap.get(variableId);
        waitlist.add(0, operation);
    }

    /**
     * Add operation from the head of the wait list
     * @param variableId variable of the waiting operation waiting for access
     * @return operation that's removed
     */
    private Operation removeOperaionFromWaitlistHead(int variableId) {
        List<Operation> waitlist = variableWaitlistMap.get(variableId);
        return waitlist.remove(0);
    }
}
