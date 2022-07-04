package com.sics.rock.tableinsight4.procedure.interval;

import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * interval
 * (left, right)
 * (left, right]
 * [left, right)
 * [left, right]
 */
public class FInterval implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FInterval.class);

    private static final String LE = "<";
    private static final String LEQ = "<=";
    private static final String GT = ">";
    private static final String GEQ = ">=";
    private static final String LEFT_OPEN = "(";
    private static final String RIGHT_OPEN = ")";
    private static final String LEFT_CLOSE = "[";
    private static final String RIGHT_CLOSE = "]";

    private final FConstant<Double> left;

    private final FConstant<Double> right;

    /**
     * left in the interval if true
     */
    private final boolean leftClose;

    /**
     * right in the interval if true
     */
    private final boolean rightClose;

    public FInterval(double left, double right, boolean leftClose, boolean rightClose) {
        this.left = new FConstant<>(left);
        this.right = new FConstant<>(right);
        this.leftClose = leftClose;
        this.rightClose = rightClose;

        FAssertUtils.require(!Double.isNaN(left), "Interval " + toString() + " is invalid");
        FAssertUtils.require(!Double.isNaN(right), "Interval " + toString() + " is invalid");
        FAssertUtils.require(!(Double.isInfinite(left) && Double.isInfinite(right)), "Interval " + toString() + " is meaningless");

        if (Double.isInfinite(left) && leftClose) logger.warn("Interval {} left is inf but close.", this);
        if (Double.isInfinite(right) && rightClose) logger.warn("Interval {} left is inf but close.", this);

        FAssertUtils.require(left <= right, "Interval " + toString() + " is invalid");
    }

    /**
     * parse interval [x, y], (x, y] ...
     */
    public static FInterval parse(String interval) throws IllegalArgumentException {
        final double left;
        final double right;
        final boolean leftEq;
        final boolean rightEq;

        interval = interval.trim();

        if (interval.startsWith(LEFT_OPEN)) {
            leftEq = false;
            interval = interval.substring(LEFT_OPEN.length());
        } else if (interval.startsWith(LEFT_CLOSE)) {
            leftEq = true;
            interval = interval.substring(LEFT_CLOSE.length());
        } else throw new IllegalArgumentException("Invalid interval string " + interval);

        if (interval.endsWith(RIGHT_OPEN)) {
            rightEq = false;
            interval = interval.substring(0, interval.length() - RIGHT_OPEN.length());
        } else if (interval.endsWith(RIGHT_CLOSE)) {
            rightEq = true;
            interval = interval.substring(0, interval.length() - RIGHT_CLOSE.length());
        } else throw new IllegalArgumentException("Invalid interval string " + interval);

        final String[] split = interval.split(",");
        if (split.length != 2) throw new IllegalArgumentException("Invalid interval string " + interval);

        try {
            left = Double.parseDouble(split[0]);
            right = Double.parseDouble(split[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid interval string " + interval, e);
        }

        return new FInterval(left, right, leftEq, rightEq);
    }

    /**
     * 1. "a number". Create intervals (-Inf, num] and (num, +inf)
     * 2. "op number". Like ">5", ">=10", "≤20"
     * 3. "interval". Like "[3,5]", "(12,30]"
     */
    public static List<FInterval> of(String intervalLike) throws IllegalArgumentException {
        if (StringUtils.isBlank(intervalLike)) throw new IllegalArgumentException(intervalLike + " is blank");
        intervalLike = intervalLike.trim();
        if (intervalLike.startsWith(LEFT_OPEN) || intervalLike.startsWith(LEFT_CLOSE)) {
            return Collections.singletonList(parse(intervalLike));
        } else if (intervalLike.startsWith(LEQ)) {
            double number = Double.parseDouble(intervalLike.substring(LEQ.length()));
            if (Double.isInfinite(number)) throw new IllegalArgumentException(intervalLike + " is infinite");
            if (Double.isNaN(number)) throw new IllegalArgumentException(intervalLike + " is NaN");
            return Collections.singletonList(new FInterval(Double.NEGATIVE_INFINITY, number, false, true));
        } else if (intervalLike.startsWith(GEQ)) {
            double number = Double.parseDouble(intervalLike.substring(GEQ.length()));
            if (Double.isInfinite(number)) throw new IllegalArgumentException(intervalLike + " is infinite");
            if (Double.isNaN(number)) throw new IllegalArgumentException(intervalLike + " is NaN");
            return Collections.singletonList(new FInterval(number, Double.POSITIVE_INFINITY, true, false));
        } else if (intervalLike.startsWith(LE)) {
            double number = Double.parseDouble(intervalLike.substring(LE.length()));
            if (Double.isInfinite(number)) throw new IllegalArgumentException(intervalLike + " is infinite");
            if (Double.isNaN(number)) throw new IllegalArgumentException(intervalLike + " is NaN");
            return Collections.singletonList(new FInterval(Double.NEGATIVE_INFINITY, number, false, false));
        } else if (intervalLike.startsWith(GT)) {
            double number = Double.parseDouble(intervalLike.substring(GT.length()));
            if (Double.isInfinite(number)) throw new IllegalArgumentException(intervalLike + " is infinite");
            if (Double.isNaN(number)) throw new IllegalArgumentException(intervalLike + " is NaN");
            return Collections.singletonList(new FInterval(number, Double.POSITIVE_INFINITY, false, false));
        } else {
            double number = Double.parseDouble(intervalLike);
            if (Double.isInfinite(number)) throw new IllegalArgumentException(intervalLike + " is infinite");
            if (Double.isNaN(number)) throw new IllegalArgumentException(intervalLike + " is NaN");
            return FUtils.listOf(
                    new FInterval(Double.NEGATIVE_INFINITY, number, false, true),
                    new FInterval(number, Double.POSITIVE_INFINITY, false, false)
            );
        }
    }

    /**
     * Relative inequality
     * [a, b] + mid => a ≤ mid ≤ b
     * (-Inf, b) + mid => mid < b
     */
    private String inequalityOf(String middle, int maxDecimalPlace, boolean allowExponentialForm) {
        final String l = FUtils.round(left.getConstant(), maxDecimalPlace, allowExponentialForm);
        final String r = FUtils.round(right.getConstant(), maxDecimalPlace, allowExponentialForm);

        if (Double.isInfinite(left.getConstant())) {
            return middle + " " + lessInequality(rightClose) + " " + r;
        } else if (Double.isInfinite(right.getConstant())) {
            return middle + " " + greatInequality(leftClose) + " " + l;
        } else {
            return l + " " + lessInequality(leftClose) + " " + middle + " " + lessInequality(rightClose) + " " + r;
        }
    }

    public String inequalityOf(String middle) {
        return inequalityOf(middle, -1, true);
    }

    /**
     * Split Relative inequality
     * [a, b] + mid => mid ≥ a, mid ≤ b
     */
    public List<String> splitInequalityOf(String middle, int maxDecimalPlace, boolean allowExponentialForm) {
        final String l = FUtils.round(left.getConstant(), maxDecimalPlace, allowExponentialForm);
        final String r = FUtils.round(right.getConstant(), maxDecimalPlace, allowExponentialForm);

        List<String> ret = new ArrayList<>(2);
        if (!Double.isInfinite(left.getConstant())) {
            ret.add(middle + " " + greatInequality(leftClose) + " " + l);
        }

        if (!Double.isInfinite(right.getConstant())) {
            ret.add(middle + " " + lessInequality(rightClose) + " " + r);
        }

        return ret;
    }

    public String toString(int maxDecimalPlace, boolean allowExponentialForm) {
        return leftBoundary() + FUtils.round(left.getConstant(), maxDecimalPlace, allowExponentialForm)
                + ", " + FUtils.round(right.getConstant(), maxDecimalPlace, allowExponentialForm) + rightBoundary();
    }

    @Override
    public String toString() {
        return leftBoundary() + left + ", " + right + rightBoundary();
    }

    private String leftBoundary() {
        return leftClose ? LEFT_CLOSE : LEFT_OPEN;
    }

    private static String lessInequality(boolean close) {
        return close ? LEQ : LE;
    }

    private static String greatInequality(boolean close) {
        return close ? GEQ : GT;
    }

    private String rightBoundary() {
        return rightClose ? RIGHT_CLOSE : RIGHT_OPEN;
    }


    public boolean including(double number) {
        return (Double.isInfinite(left.getConstant()) || (leftClose ? (number >= left.getConstant()) : (number > left.getConstant())))
                &&
                (Double.isInfinite(right.getConstant()) || (rightClose ? (number <= right.getConstant()) : (number < right.getConstant())));
    }

    public List<FConstant<Double>> constants() {
        return FUtils.listOf(left, right);
    }
}
