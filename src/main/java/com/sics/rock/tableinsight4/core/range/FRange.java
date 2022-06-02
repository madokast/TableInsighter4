package com.sics.rock.tableinsight4.core.range;

import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Range
 * (left, right)
 * (left, right]
 * [left, right)
 * [left, right]
 * <p>
 * TODO wait for test
 */
public class FRange implements Serializable {

    private static final String LE = "<";
    private static final String LEQ = "≤";
    private static final String GT = ">";
    private static final String GEQ = "≥";
    private static final String LEFT_OPEN = "(";
    private static final String RIGHT_OPEN = ")";
    private static final String LEFT_CLOSE = "[";
    private static final String RIGHT_CLOSE = "]";

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

        FAssertUtils.require(!Double.isNaN(left), "Range " + toString() + " is invalid");
        FAssertUtils.require(!Double.isNaN(right), "Range " + toString() + " is invalid");
        FAssertUtils.require(!(Double.isInfinite(left) && Double.isInfinite(right)), "Range " + toString() + " is meaningless");
        FAssertUtils.require(left <= right, "Range " + toString() + " is invalid");
    }

    public static FRange parse(String range) {
        final double left;
        final double right;
        final boolean leftEq;
        final boolean rightEq;

        range = range.trim();

        if (range.startsWith(LEFT_OPEN)) {
            leftEq = false;
            range = range.substring(LEFT_OPEN.length());
        } else if (range.startsWith(LEFT_CLOSE)) {
            leftEq = true;
            range = range.substring(LEFT_CLOSE.length());
        } else throw new IllegalArgumentException("Invalid range string " + range);

        if (range.startsWith(RIGHT_OPEN)) {
            rightEq = false;
            range = range.substring(0, range.length() - RIGHT_OPEN.length());
        } else if (range.startsWith(RIGHT_CLOSE)) {
            rightEq = true;
            range = range.substring(0, range.length() - RIGHT_CLOSE.length());
        } else throw new IllegalArgumentException("Invalid range string " + range);

        final String[] split = range.split(", ");
        if (split.length != 2) throw new IllegalArgumentException("Invalid range string " + range);

        try {
            left = Double.parseDouble(split[0]);
            right = Double.parseDouble(split[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid range string " + range, e);
        }

        return new FRange(left, right, leftEq, rightEq);
    }

    private List<String> splitInequalityOf(String middle, int maxDecimalPlace, boolean allowExponentialForm) {
        final String l = FUtils.round(left, maxDecimalPlace, allowExponentialForm);
        final String r = FUtils.round(right, maxDecimalPlace, allowExponentialForm);

        List<String> ret = new ArrayList<>(2);
        if (!Double.isInfinite(left)) {
            ret.add(middle + " " + leftInverseInequality() + " " + l);
        }

        if (!Double.isInfinite(right)) {
            ret.add(middle + " " + rightInequality() + "" + r);
        }

        return ret;
    }

    private String inequalityOf(String middle, int maxDecimalPlace, boolean allowExponentialForm) {
        final String l = FUtils.round(left, maxDecimalPlace, allowExponentialForm);
        final String r = FUtils.round(right, maxDecimalPlace, allowExponentialForm);

        if (Double.isInfinite(left)) {
            return middle + " " + rightInequality() + "" + r;
        } else if (Double.isInfinite(right)) {
            return middle + " " + leftInverseInequality() + " " + l;
        } else {
            return l + " " + leftInequality() + " " + middle + " " + rightInequality() + "" + r;
        }
    }

    public String inequalityOf(String middle) {
        return inequalityOf(middle, -1, true);
    }

    public String toString(int maxDecimalPlace, boolean allowExponentialForm) {
        return leftBoundary() + FUtils.round(left, maxDecimalPlace, allowExponentialForm)
                + ", " + FUtils.round(right, maxDecimalPlace, allowExponentialForm) + rightBoundary();
    }

    @Override
    public String toString() {
        return leftBoundary() + left + ", " + right + rightBoundary();
    }

    private String leftBoundary() {
        return leftEq ? LEFT_CLOSE : LEFT_OPEN;
    }

    private String leftInequality() {
        return leftEq ? LEQ : LE;
    }

    private String leftInverseInequality() {
        return leftEq ? GEQ : GT;
    }

    private String rightInequality() {
        return rightEq ? GEQ : GT;
    }

    private String rightBoundary() {
        return rightEq ? RIGHT_CLOSE : RIGHT_OPEN;
    }


    public boolean including(double number) {
        return (Double.isInfinite(left) || (leftEq ? (number >= left) : (number > left)))
                &&
                (Double.isInfinite(right) || (rightEq ? (number <= right) : (number < right)));
    }
}
