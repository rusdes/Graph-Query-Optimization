package operators.flinkdependencies;


import operators.flinkdependencies.Public;
 
 /**
  * The base interface for all user-defined functions.
  *
  * <p>This interface is empty in order to allow extending interfaces to be SAM (single abstract
  * method) interfaces that can be implemented via Java 8 lambdas.
  */
 @Public
 public interface Function extends java.io.Serializable {}
