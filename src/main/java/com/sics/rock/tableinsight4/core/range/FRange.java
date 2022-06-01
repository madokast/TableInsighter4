package com.sics.rock.tableinsight4.core.range;

import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FUtils;

/**
 * Range
 * (left, right)
 * (left, right]
 * [left, right)
 * [left, right]
 */
public class FRange {

    private final double left;

    private final double right;

    /**
     * left in the range if true
     */
    private final boolean leftEq;

    /**
     * right in the range if true
     */
    private final boolean rightEq;

    public FRange(double left, double right, boolean leftEq, boolean rightEq) {
        this.left = left;
        this.right = right;
        this.leftEq = leftEq;
        this.rightEq = rightEq;
        FAssertUtils.require(left <= right, "Range " + toString() + " is invalid");
    }

    public static FRange parse(String range) {
        // TODO
        return null;
    }

    public String inequalityOf(String middle, int maxDecimalPlace, boolean allowExponentialForm) {
        final String l = FUtils.round(left, maxDecimalPlace, allowExponentialForm);
        final String r = FUtils.round(right, maxDecimalPlace, allowExponentialForm);
        return l + " " + leftInequality() + " " + middle + " " + rightInequality() + "" + r;
    }

    public String inequalityOf(String middle) {
        return inequalityOf(middle, -1, true);
    }

    public String toString(int maxDecimalPlace, boolean allowExponentialForm) {
        // TODO
        return toString();
    }

    @Override
    public String toString() {
        return leftBoundary() + leftEq + ", " + rightEq + rightBoundary();
    }

    public String leftBoundary() {
        return leftEq ? "[" : "(";
    }

    public String leftInequality() {
        return leftEq ? "≤" : "<";
    }

    public String rightInequality() {
        return rightEq ? "≥" : ">";
    }

    public String rightBoundary() {
        return rightEq ? "]" : ")";
    }

}
