package operators.helper;

/**
 * Collects a record and forwards it. The collector is the "push" counterpart of the {@link
* java.util.Iterator}, which "pulls" data in.
*/
@Public
public interface Collector<T> {

    /**
     * Emits a record.
    *
    * @param record The record to collect.
    */
    void collect(T record);

    /** Closes the collector. If any data was buffered, that data will be flushed. */
    void close();
}