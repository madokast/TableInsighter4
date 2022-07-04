package com.sics.rock.tableinsight4.predicate;

public enum FOperator {

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
}
