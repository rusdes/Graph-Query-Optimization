
package operators.helper;

@Public
@FunctionalInterface
public interface FilterFunction<T> extends Function {

    /**
     * The filter function that evaluates the predicate.
     *
     * <p>
     * <strong>IMPORTANT:</strong> The system assumes that the function does not
     * modify the
     * elements on which the predicate is applied. Violating this assumption can
     * lead to incorrect
     * results.
     *
     * @param value The value to be filtered.
     * @return True for values that should be retained, false for values to be
     *         filtered out.
     * @throws Exception This method may throw exceptions. Throwing an exception
     *                   will cause the
     *                   operation to fail and may trigger recovery.
     */
    boolean filter(T value) throws Exception;
}