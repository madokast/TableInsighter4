package com.sics.rock.tableinsight4.preprocessing.interval;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * interval
 * (left, right)
 * (left, right]
 * [left, right)
 * [left, right]
 *
 * @author zhaorx
 */
public class FInterval implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FInterval.class);

    private static final String LE = "<";
    private static final String LEQ = "<=";
    private static final String __LEQ = "≤";
    private static final String GT = ">";
    private static final String GEQ = ">=";
    private static final String __GEQ = "≥";
    private static final String LEFT_OPEN = "(";
    private static final String RIGHT_OPEN = ")";
    private static final String LEFT_CLOSE = "[";
    private static final String RIGHT_CLOSE = "]";

    private final FConstant<Comparable> left;

    private final FConstant<Comparable> right;

    /**
     * left in the interval if true
     */
    private final boolean leftClose;

    /**
     * right in the interval if true
     */
    private final boolean rightClose;

    public FInterval(Comparable left, Comparable right, boolean leftClose, boolean rightClose) {

        this.left = left == null ? FConstant.NEGATIVE_INFINITY : FConstant.of(left);
        this.right = right == null ? FConstant.POSITIVE_INFINITY : FConstant.of(right);

        this.leftClose = leftClose;
        this.rightClose = rightClose;
    }

    /**
     * parse interval [x, y], (x, y] ...
     */
    public static FInterval parse(String interval, FValueType type) throws IllegalArgumentException {
        final Comparable left;
        final Comparable right;
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
            left = (Comparable) type.cast(split[0]);
            right = (Comparable) type.cast(split[1]);

            if (left == null) throw new IllegalArgumentException(split[0] + " cannot cast to " + type);
            if (right == null) throw new IllegalArgumentException(split[1] + " cannot cast to " + type);
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
    public static List<FInterval> of(String intervalLike, FValueType type) throws IllegalArgumentException {
        if (StringUtils.isBlank(intervalLike)) throw new IllegalArgumentException(intervalLike + " is blank");
        intervalLike = intervalLike.trim();
        if (intervalLike.startsWith(LEFT_OPEN) || intervalLike.startsWith(LEFT_CLOSE)) {
            return Collections.singletonList(parse(intervalLike, type));
        } else if (intervalLike.startsWith(LEQ)) {
            Comparable number = (Comparable) type.cast(intervalLike.substring(LEQ.length()).trim());
            return Collections.singletonList(new FInterval(Double.NEGATIVE_INFINITY, number, false, true));
        } else if (intervalLike.startsWith(GEQ)) {
            Comparable number = (Comparable) type.cast(intervalLike.substring(GEQ.length()).trim());
            return Collections.singletonList(new FInterval(number, Double.POSITIVE_INFINITY, true, false));
        } else if (intervalLike.startsWith(__LEQ)) {
            Comparable number = (Comparable) type.cast(intervalLike.substring(__LEQ.length()).trim());
            return Collections.singletonList(new FInterval(Double.NEGATIVE_INFINITY, number, false, true));
        } else if (intervalLike.startsWith(__GEQ)) {
            Comparable number = (Comparable) type.cast(intervalLike.substring(__GEQ.length()).trim());
            return Collections.singletonList(new FInterval(number, Double.POSITIVE_INFINITY, true, false));
        } else if (intervalLike.startsWith(LE)) {
            Comparable number = (Comparable) type.cast(intervalLike.substring(LE.length()).trim());
            return Collections.singletonList(new FInterval(Double.NEGATIVE_INFINITY, number, false, false));
        } else if (intervalLike.startsWith(GT)) {
            Comparable number = (Comparable) type.cast(intervalLike.substring(GT.length()).trim());
            return Collections.singletonList(new FInterval(number, Double.POSITIVE_INFINITY, false, false));
        } else {
            Comparable number = (Comparable) type.cast(intervalLike);
            return FTiUtils.listOf(
                    new FInterval(Double.NEGATIVE_INFINITY, number, false, true),
                    new FInterval(number, Double.POSITIVE_INFINITY, false, false)
            );
        }
    }

    public void validate() {
        final Optional<FConstant<?>> left = this.left();
        final Optional<FConstant<?>> right = this.right();
        if (!left.isPresent()) {
            FAssertUtils.require(right::isPresent, () -> "The interval " + this + " has no bound.");
            FAssertUtils.require(right.get().getIndex() != FConstant.INDEX_NOT_INIT, () -> "Interval " + this + " constant index not init!");
        } else if (!right.isPresent()) {
            FAssertUtils.require(left.get().getIndex() != FConstant.INDEX_NOT_INIT, () -> "Interval " + this + " constant index not init!");
        } else {
            FAssertUtils.require(left.get().getIndex() != FConstant.INDEX_NOT_INIT, () -> "Interval " + this + " constant index not init!");
            FAssertUtils.require(right.get().getIndex() != FConstant.INDEX_NOT_INIT, () -> "Interval " + this + " constant index not init!");
        }
    }

    /**
     * Relative inequality
     * [a, b] + mid => a ≤ mid ≤ b
     * (-Inf, b) + mid => mid < b
     */
    public String inequalityOf(String middle, int maxDecimalPlace, boolean allowExponentialForm) {
        if (left.equals(FConstant.NEGATIVE_INFINITY)) {
            String r = rightString(maxDecimalPlace, allowExponentialForm);
            return middle + " " + lessInequality(rightClose) + " '" + r + "'";
        } else if (right.equals(FConstant.POSITIVE_INFINITY)) {
            String l = leftString(maxDecimalPlace, allowExponentialForm);
            return middle + " " + greatInequality(leftClose) + " '" + l + "'";
        } else {
            String r = rightString(maxDecimalPlace, allowExponentialForm);
            String l = leftString(maxDecimalPlace, allowExponentialForm);
            return l + " '" + lessInequality(leftClose) + "' " + middle + " '" + lessInequality(rightClose) + "' " + r;
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
        List<String> ret = new ArrayList<>(2);

        if (!left.equals(FConstant.NEGATIVE_INFINITY)) {
            String l = leftString(maxDecimalPlace, allowExponentialForm);
            ret.add(middle + " " + greatInequality(leftClose) + " " + l);
        }

        if (!right.equals(FConstant.POSITIVE_INFINITY)) {
            String r = rightString(maxDecimalPlace, allowExponentialForm);
            ret.add(middle + " " + lessInequality(rightClose) + " " + r);
        }

        return ret;
    }

    public String toString(int maxDecimalPlace, boolean allowExponentialForm) {
        return leftBoundary() + leftString(maxDecimalPlace, allowExponentialForm)
                + ", " + rightString(maxDecimalPlace, allowExponentialForm) + rightBoundary();
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

    public Optional<FConstant<?>> left() {
        return left.equals(FConstant.NEGATIVE_INFINITY) ? Optional.empty() : Optional.of(left);
    }

    public Optional<FConstant<?>> right() {
        return right.equals(FConstant.POSITIVE_INFINITY) ? Optional.empty() : Optional.of(right);
    }

    public List<FConstant<?>> constants() {
        List<FConstant<?>> ret = new ArrayList<>(2);
        left().ifPresent(ret::add);
        right().ifPresent(ret::add);
        return ret;
    }

    public boolean leftClose() {
        return leftClose;
    }

    public boolean rightClose() {
        return rightClose;
    }


    public FOperator leftOperator() {
        return leftClose ? FOperator.GET : FOperator.GT;
    }

    public FOperator rightOperator() {
        return rightClose ? FOperator.LET : FOperator.LT;
    }

    private String leftString(int maxDecimalPlace, boolean allowExponentialForm) {
        if (left.equals(FConstant.NEGATIVE_INFINITY)) {
            return left.getConstant().toString();
        } else {
            Comparable lc = left.getConstant();
            if (lc instanceof Double || lc instanceof Float) {
                return FTiUtils.round(((Number) lc).doubleValue(), maxDecimalPlace, allowExponentialForm);
            } else {
                return lc.toString();
            }
        }
    }

    public Optional<FInterval> typeCast(FValueType type) {
        Comparable<?> leftConst, rightConst;

        if (left().isPresent()) {
            leftConst = (Comparable<?>) type.cast(this.left.getConstant());
            if (leftConst == null)
                leftConst = (Comparable<?>) type.cast(FValueType.DOUBLE.cast(this.left.getConstant()));
        } else {
            leftConst = null;
        }

        if (right().isPresent()) {
            rightConst = (Comparable<?>) type.cast(this.right.getConstant());
            if (rightConst == null)
                rightConst = (Comparable<?>) type.cast(FValueType.DOUBLE.cast(this.right.getConstant()));
        } else {
            rightConst = null;
        }

        if (leftConst == null && rightConst == null) {
            logger.info("Cannot cast {} to {}", this, type);
            return Optional.empty();
        } else {
            FInterval des = new FInterval(leftConst, rightConst, this.leftClose, this.rightClose);
            logger.info("Interval {} cast to {} as {}", this, type, des);
            return Optional.of(des);
        }
    }

    private String rightString(int maxDecimalPlace, boolean allowExponentialForm) {
        if (right.equals(FConstant.POSITIVE_INFINITY)) {
            return right.getConstant().toString();
        } else {
            Comparable rc = right.getConstant();
            if (rc instanceof Double || rc instanceof Float) {
                return FTiUtils.round(((Number) rc).doubleValue(), maxDecimalPlace, allowExponentialForm);
            } else {
                return rc.toString();
            }
        }
    }

    // test only
    public boolean including(Comparable v) {
        if (left.equals(FConstant.NEGATIVE_INFINITY)) {
            return right.getConstant().compareTo(v) >= (rightClose ? 0 : 1);
        } else if (right.equals(FConstant.POSITIVE_INFINITY)) {
            return v.compareTo(left.getConstant()) >= (leftClose ? 0 : 1);
        } else {
            return right.getConstant().compareTo(v) >= (rightClose ? 0 : 1) &&
                    v.compareTo(left.getConstant()) >= (leftClose ? 0 : 1);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FInterval fInterval = (FInterval) o;
        return leftClose == fInterval.leftClose &&
                rightClose == fInterval.rightClose &&
                Objects.equals(left, fInterval.left) &&
                Objects.equals(right, fInterval.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, leftClose, rightClose);
    }
}
