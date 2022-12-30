package operators.helper;

@Public
@FunctionalInterface
public interface JoinFunction<IN1, IN2, OUT> extends Function {

    /**
     * The join method, called once per joined pair of elements.
    *
    * @param first The element from first input.
    * @param second The element from second input.
    * @return The resulting element.
    * @throws Exception This method may throw exceptions. Throwing an exception will cause the
    *     operation to fail and may trigger recovery.
    */
    OUT join(IN1 first, IN2 second) throws Exception;
}