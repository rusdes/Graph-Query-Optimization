package operators.helper;



import operators.helper.Public;
import operators.helper.Collector;

import java.io.Serializable;

@Public
@FunctionalInterface
public interface FlatJoinFunction<IN1, IN2, OUT> extends Function, Serializable {

    /**
     * The join method, called once per joined pair of elements.
    *
    * @param first The element from first input.
    * @param second The element from second input.
    * @param out The collector used to return zero, one, or more elements.
    * @throws Exception This method may throw exceptions. Throwing an exception will cause the
    *     operation to fail and may trigger recovery.
    */
    void join(IN1 first, IN2 second, Collector<OUT> out) throws Exception;
}