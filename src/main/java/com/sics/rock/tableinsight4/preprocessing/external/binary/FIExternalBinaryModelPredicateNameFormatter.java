package com.sics.rock.tableinsight4.preprocessing.external.binary;

import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;

import java.util.List;

/**
 * similar('levenshtein' , t0.ac, t1.ac, 0.85)
 * ML('ditto', t0.ac, t1.ac)
 *
 * @author zhaorx
 */
public interface FIExternalBinaryModelPredicateNameFormatter {

    String format(int leftTupleId, int rightTupleId);

    /**
     * similar('levenshtein' , t0.ac, t1.ac, 0.85)
     */
    class FSimilarPredicateNameFormatter implements FIExternalBinaryModelPredicateNameFormatter {

        private final FDerivedColumnNameHandler derivedColumnNameHandler;
        private final List<String> leftColumnNames;
        private final List<String> rightColumnNames;
        private final String algorithmName;
        private final String threshold;

        public FSimilarPredicateNameFormatter(FDerivedColumnNameHandler derivedColumnNameHandler,
                                              List<String> leftColumnNames, List<String> rightColumnNames,
                                              String algorithmName, String threshold) {
            this.derivedColumnNameHandler = derivedColumnNameHandler;
            this.leftColumnNames = leftColumnNames;
            this.rightColumnNames = rightColumnNames;
            this.algorithmName = algorithmName;
            this.threshold = threshold;
        }

        @Override
        public String format(int leftTupleId, int rightTupleId) {
            String leftCol = derivedColumnNameHandler.deriveCombinedColumn(leftColumnNames);
            String rightCol = derivedColumnNameHandler.deriveCombinedColumn(rightColumnNames);
            return String.format("similar('%s' , t%d.%s, t%d.%s, %s)",
                    algorithmName, leftTupleId, leftCol, rightTupleId, rightCol, threshold);
        }
    }

    /**
     * ML('ditto', t0.ac, t1.ac)
     */
    class FMachineLearningPredicateNameFormatter implements FIExternalBinaryModelPredicateNameFormatter {
        private final FDerivedColumnNameHandler derivedColumnNameHandler;
        private final List<String> leftColumnNames;
        private final List<String> rightColumnNames;
        private final String algorithmName;

        public FMachineLearningPredicateNameFormatter(FDerivedColumnNameHandler derivedColumnNameHandler,
                                                      List<String> leftColumnNames, List<String> rightColumnNames,
                                                      String algorithmName) {
            this.derivedColumnNameHandler = derivedColumnNameHandler;
            this.leftColumnNames = leftColumnNames;
            this.rightColumnNames = rightColumnNames;
            this.algorithmName = algorithmName;
        }

        @Override
        public String format(int leftTupleId, int rightTupleId) {
            String leftCol = derivedColumnNameHandler.deriveCombinedColumn(leftColumnNames);
            String rightCol = derivedColumnNameHandler.deriveCombinedColumn(rightColumnNames);
            return String.format("ML('%s' , t%d.%s, t%d.%s)",
                    algorithmName, leftTupleId, leftCol, rightTupleId, rightCol);
        }
    }

}
