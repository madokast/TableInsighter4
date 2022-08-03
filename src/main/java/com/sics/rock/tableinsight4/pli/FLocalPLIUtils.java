package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.predicate.FOperator;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Utils of PLI
 *
 * @author zhaorx
 */
public class FLocalPLIUtils {

    public static Stream<FPair<List<Integer>, Stream<Integer>>> localRowIdsOf(
            FLocalPLI leftLocalPLI, FLocalPLI rightLocalPLI, FOperator operator) {

        final Map<Long, List<Integer>> leftPLI = leftLocalPLI.getIndex2localRowIds();
        final Map<Long, List<Integer>> rightPLI = rightLocalPLI.getIndex2localRowIds();

        switch (operator) {
            case EQ:
                return leftPLI.entrySet().stream()
                        .filter(e -> rightPLI.containsKey(e.getKey()))
                        .map(e -> {
                            Long index = e.getKey();
                            List<Integer> leftRowIds = e.getValue();
                            List<Integer> rightRowIds = rightPLI.get(index);
                            return new FPair<>(leftRowIds, rightRowIds.stream());
                        });
            case GT:
            case LT:
            case GET:
            case LET:
                return leftPLI.entrySet().stream()
                        // non-negative index for comparing
                        .filter(e -> e.getKey() >= 0L)
                        .map(e -> {
                            Long index = e.getKey();
                            List<Integer> leftRowIds = e.getValue();
                            Stream<Integer> rightRowIds = rightLocalPLI.localRowIdsOf(index, operator.symmetric());
                            return new FPair<>(leftRowIds, rightRowIds);
                        });
            default:
                throw new RuntimeException("Operator " + operator + " not support");
        }
    }

}
