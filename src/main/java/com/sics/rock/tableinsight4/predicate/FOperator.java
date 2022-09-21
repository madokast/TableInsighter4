package com.sics.rock.tableinsight4.predicate;

import java.io.Serializable;

/**
 * operator used in predicate
 *
 * @author zhaorx
 */
public enum FOperator implements Serializable {

    EQ("="),

    LT("<"),

    GT(">"),

    LET("<="),

    GET(">="),

    BELONG("∈");

    public final String symbol;

    FOperator(String symbol) {
        this.symbol = symbol;
    }

    FOperator of(String symbol) {
        for (FOperator operator : FOperator.values()) {
            if (operator.symbol.equals(symbol)) return operator;
        }

        throw new IllegalArgumentException("No operator matches symbol " + symbol);
    }

    /**
     * if a op1 b iff. b op2 a,
     * then op1 and op2 are symmetric.
     */
    public FOperator symmetric() {
        switch (this) {
            case EQ:
                return EQ;
            case LT:
                return GT;
            case GT:
                return LT;
            case LET:
                return GET;
            case GET:
                return LET;
            default:
                throw new IllegalArgumentException("Operator of " + this + " has no symmetric counterpart.");
        }
    }
}
