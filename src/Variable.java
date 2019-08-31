import java.util.Objects;
import java.util.TreeMap;

/**
 * This class holds the value of the variables and keeps
 * the history values. It also has a status indicator
 * helping with site recovery.
 *
 * @author Weiqiang Li
 * Updated: 12/06/2018
 */
public class Variable {

    private final int id; // variable id
    private int value; // variable value
    private long updateTime; // last update time
    private boolean readable; // if it is readable
    private TreeMap<Long, Integer> previousValues; // all previous values

    public Variable(int id) {
        this.id = id;
        this.value = 10 * id;
        this.updateTime = 0;
        this.readable = true;
        this.previousValues = new TreeMap<>();
        previousValues.put(updateTime, value);
    }

    /**
     * Update the value of this variable and archive the old value.
     * @param value - new value
     */
    public void updateValue(int value, long updateTime) {
        this.value = value;
        this.updateTime = System.currentTimeMillis();
        this.readable = true;
        previousValues.put(updateTime, value);
    }

    /**
     * Get the current value.
     * @return current value
     */
    public int getValue() {
        return this.value;
    }

    /**
     * Get variable id.
     * @return id
     */
    public int getId() {
        return this.id;
    }

    /**
     * Get the last update time.
     * @return update time
     */
    public long getUpdateTime() {
        return this.updateTime;
    }

    /**
     * Check if it is readable.
     * @return true if it can be read
     */
    public boolean isReadable() {
        return this.readable;
    }

    /**
     * Mark it unreadable.
     */
    public void fail() {
        this.readable = false;
    }

    /**
     * Get the value before a specific time.
     * @param time
     * @return the value before that time
     */
    public int getValueBeforeTime(Long time) {
        return previousValues.floorEntry(time).getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Variable variable = (Variable) o;
        return id == variable.id &&
                value == variable.value &&
                updateTime == variable.updateTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, value, updateTime);
    }
}
