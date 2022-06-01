package com.sics.rock.tableinsight4.table.column;

/**
 * Type of table column
 *
 * @author zhaorx
 */
public enum FColumnType {

    /**
     * The user-defined id column,
     * for offering results of model predicates, and
     * for giving positive and negative examples of rules
     */
    ID,

    /**
     * The derived ordered id column.
     * Start with 0 and continuity 0, 1, 2...
     * Used for indexing and work smoothly with spark.
     */
    ORDERED_ID,

    /**
     * Normal column.
     */
    NORMAL,

    /**
     * Derived range column. Like
     * age  $RG{age}
     *  25   (0,30]
     *  35   (30,50]
     *  40   (30,50]
     *
     * The "age" is a normal column
     * and the "$RG{age}" is a derived range column from "age".
     * For generating range predicate and relative rules.
     */
    RANGE,

    /**
     * Derived external binary model column. Like
     * address                                              $EX_001
     * 1600 Pennsylvania Avenue, Washington                 0
     * 1600 Pennsylvania Avenue, Washington, DC             0
     * Cedars-Sinai Medical Center, RD                      1
     * Cedars-Sinai Medical Center                          1
     *
     * The column "$EX_001" derived by results of external binary model,
     * tell the address is the same or not.
     */
    EXTERNAL_BINARY_MODEL

}
