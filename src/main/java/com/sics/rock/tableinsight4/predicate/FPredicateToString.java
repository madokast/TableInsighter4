package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.predicate.impl.*;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;

/**
 * utils for predicate to string
 *
 * @author zhaorx
 */
public class FPredicateToString implements FTiEnvironment {

    public final FDerivedColumnNameHandler derivedColumnNameHandler;

    public String toString(FIPredicate predicate) {
        if (predicate instanceof FUnaryConsPredicate) {
            return ((FUnaryConsPredicate) predicate).toString(config().constantNumberMaxDecimalPlace,
                    config().constantNumberAllowExponentialForm);
        } else if (predicate instanceof FUnaryIntervalConsPredicate) {
            return ((FUnaryIntervalConsPredicate) predicate).toString(
                    config().constantNumberMaxDecimalPlace, config().constantNumberAllowExponentialForm);
        } else if (predicate instanceof FBinaryModelPredicate) {
            return ((FBinaryModelPredicate) predicate).toString(derivedColumnNameHandler);
        } else if (predicate instanceof FBinaryConsPredicate) {
            return ((FBinaryConsPredicate) predicate).toString(config().syntaxConjunction,
                    config().constantNumberMaxDecimalPlace, config().constantNumberAllowExponentialForm);
        } else if (predicate instanceof FBinaryIntervalConsPredicate) {
            return ((FBinaryIntervalConsPredicate) predicate).toString(
                    config().constantNumberMaxDecimalPlace, config().constantNumberAllowExponentialForm, config().syntaxConjunction);
        } else {
            return predicate.toString();
        }
    }

    public FPredicateToString(final FDerivedColumnNameHandler derivedColumnNameHandler) {
        this.derivedColumnNameHandler = derivedColumnNameHandler;
    }
}
