package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.evidenceset.predicateset.FExamplePredicateSet;
import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.FRddElementIndex;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.FPredicateToString;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FSparkUtils;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * rule view object
 *
 * @author zhaorx
 */
public class FRuleVO {

    private final String rule;

    private final long support;

    private final long unSupport;

    private final long rhsSupport;

    private final long rowSize;

    private final FPositiveNegativeExample supportExamples;

    private final FPositiveNegativeExample unSupportExamples;

    public FRuleVO(final String rule, final long support, final long unSupport, final long rhsSupport,
                   final long rowSize, final FPositiveNegativeExample supportExamples,
                   final FPositiveNegativeExample unSupportExamples) {
        this.rule = rule;
        this.support = support;
        this.unSupport = unSupport;
        this.rhsSupport = rhsSupport;
        this.rowSize = rowSize;
        this.supportExamples = supportExamples;
        this.unSupportExamples = unSupportExamples;
    }

    public static List<FRuleVO> create(final List<FRule> rules, final FPLI PLI, final FIEvidenceSet evidenceSet,
                                       final FPredicateIndexer predicateIndexer,
                                       final FPredicateToString predicateToString,
                                       final List<FTableInfo> tableInfoList,
                                       final String conjunctionSymbol, final String implicationSymbol,
                                       final int positiveNegativeExampleNumber) {
        final int ruleSize = rules.size();
        if (ruleSize == 0) return Collections.emptyList();

        final FPositiveNegativeExample[] supportExamplesList = new FPositiveNegativeExample[ruleSize];
        final FPositiveNegativeExample[] unSupportExamplesList = new FPositiveNegativeExample[ruleSize];
        if (positiveNegativeExampleNumber > 0) {
            fillExamplesList(rules, PLI, evidenceSet, tableInfoList, positiveNegativeExampleNumber, ruleSize, supportExamplesList, unSupportExamplesList);
        } else {
            // fill the same object since they are all empty
            Arrays.fill(supportExamplesList, new FPositiveNegativeExample());
            Arrays.fill(unSupportExamplesList, new FPositiveNegativeExample());
        }

        final long rowSize = evidenceSet.allCount();
        return IntStream.range(0, ruleSize).mapToObj(ruleId -> {
            final FRule rule = rules.get(ruleId);
            final String ruleStr = toRuleString(rule, tableInfoList, predicateIndexer, predicateToString, conjunctionSymbol,
                    implicationSymbol);
            final FPositiveNegativeExample supportExamples = supportExamplesList[ruleId];
            final FPositiveNegativeExample unSupportExamples = unSupportExamplesList[ruleId];
            return new FRuleVO(ruleStr, rule.support, rule.unSupport(), rule.ySupport(evidenceSet),
                    rowSize, supportExamples, unSupportExamples);
        }).collect(Collectors.toList());
    }

    private static String toRuleString(final FRule rule, final List<FTableInfo> tableInfoList,
                                       final FPredicateIndexer predicateIndexer,
                                       final FPredicateToString predicateToString,
                                       final String conjunctionSymbol, final String implicationSymbol) {

        final String tableDescription = IntStream.range(0, tableInfoList.size()).mapToObj(tupleId -> {
            final String tableName = tableInfoList.get(tupleId).getTableName();
            return tableName + "(" + tupleId + ")";
        }).collect(Collectors.joining(" " + conjunctionSymbol + " "));

        final String lhs = rule.xs.stream().mapToObj(predicateIndexer::getPredicate).map(predicateToString::toString)
                .collect(Collectors.joining(" " + conjunctionSymbol + " "));

        final String rh = predicateToString.toString(predicateIndexer.getPredicate(rule.y));


        return tableDescription + " " + conjunctionSymbol + " " + lhs + " " + implicationSymbol + " " + rh;
    }


    private static void fillExamplesList(final List<FRule> rules, final FPLI PLI,
                                         final FIEvidenceSet evidenceSet, final List<FTableInfo> tableInfoList,
                                         final int positiveNegativeExampleNumber, final int ruleSize,
                                         final FPositiveNegativeExample[] supportExamplesList,
                                         final FPositiveNegativeExample[] unSupportExamplesList) {
        final FPair<List<FIPredicateSet>, List<FIPredicateSet>>[] examplesList = evidenceSet.examples(rules, positiveNegativeExampleNumber);
        final Map<FTableInfo, Map<FRddElementIndex, Long>> tab2eleId2ColIdMap = createTable2ElementId2ColumnIdMap(PLI, tableInfoList, examplesList);


        for (int i = 0; i < ruleSize; i++) {
            // k is supportExamples and v is unSupportExamples
            final FPair<List<FIPredicateSet>, List<FIPredicateSet>> examples = examplesList[i];
            final FPositiveNegativeExample supportExamples = new FPositiveNegativeExample();
            final FPositiveNegativeExample unSupportExamples = new FPositiveNegativeExample();

            fillExample(supportExamples, tableInfoList, tab2eleId2ColIdMap, examples._k, positiveNegativeExampleNumber);
            fillExample(unSupportExamples, tableInfoList, tab2eleId2ColIdMap, examples._v, positiveNegativeExampleNumber);

            if (FAssertUtils.ASSERT) {
                final FRule rule = rules.get(i);

                FAssertUtils.require(supportExamples.size() == Math.min(positiveNegativeExampleNumber, rule.support),
                        () -> "Rule(" + rule + ")'s support is " + rule.support + ", but support example number is " + supportExamples.size());
                FAssertUtils.require(unSupportExamples.size() == Math.min(positiveNegativeExampleNumber, rule.unSupport()),
                        () -> "Rule(" + rule + ")'s unSupport is " + rule.unSupport() + ", but unSupport example number is " + unSupportExamples.size());
            }

            supportExamplesList[i] = supportExamples;
            unSupportExamplesList[i] = unSupportExamples;
        }
    }

    private static void fillExample(final FPositiveNegativeExample example,
                                    final List<FTableInfo> tableInfoList,
                                    final Map<FTableInfo, Map<FRddElementIndex, Long>> tab2eleId2ColIdMap,
                                    final List<FIPredicateSet> predicateSetList,
                                    final int positiveNegativeExampleNumber) {
        FILL_LOOP:
        for (final FIPredicateSet predicateSet : predicateSetList) {
            if (!(predicateSet instanceof FExamplePredicateSet)) {
                throw new RuntimeException("positiveNegativeExampleNumber is " +
                        positiveNegativeExampleNumber +
                        " greater then 0 but the predicate-set is not example-predicate-set");
            }
            final List<FRddElementIndex[]> supportIdsList = ((FExamplePredicateSet) predicateSet).getSupportIdsList();
            for (final FRddElementIndex[] elementIndices : supportIdsList) {
                final List<FPositiveNegativeExample.Row> rows = IntStream.range(0, elementIndices.length).mapToObj(tupleId -> {
                    final FRddElementIndex elementIndex = elementIndices[tupleId];
                    final FTableInfo tableInfo = tableInfoList.get(tupleId);
                    final Long colId = tab2eleId2ColIdMap.get(tableInfo).getOrDefault(elementIndex, null);
                    FAssertUtils.require(colId != null, () ->
                            "Cannot extract column-id from elementIndex. " +
                                    "Table[" + tableInfo.getTableName() + "], elementIndex[" + elementIndex + "]");
                    return FPositiveNegativeExample.Row.of(tableInfo.getTableName(), Objects.toString(colId));
                }).collect(Collectors.toList());
                example.addExample(FPositiveNegativeExample.Example.of(rows));
                if (example.size() >= positiveNegativeExampleNumber) break FILL_LOOP;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<FTableInfo, Map<FRddElementIndex, Long>> createTable2ElementId2ColumnIdMap(
            final FPLI PLI, final List<FTableInfo> tableInfoList,
            final FPair<List<FIPredicateSet>, List<FIPredicateSet>>[] examplesList) {
        return IntStream.range(0, tableInfoList.size()).mapToObj(tupleId -> {
            final FTableInfo tableInfo = tableInfoList.get(tupleId);
            final Optional<JavaPairRDD<FRddElementIndex, Long>> tableIDColumnMapOp = PLI.getTableIDColumnMap(tableInfo);
            if (!tableIDColumnMapOp.isPresent()) {
                return new FPair<>(tableInfo, (Map<FRddElementIndex, Long>) (Map) Collections.emptyMap());
            }

            // all ele indexes of the tuple-id
            final List<FRddElementIndex> allElementIndexes = Arrays.stream(examplesList).flatMap(pair -> Stream.of(pair._k, pair._v)).flatMap(List::stream)
                    .flatMap(ps -> (ps instanceof FExamplePredicateSet) ? ((FExamplePredicateSet) ps).getSupportIdsList().stream() : Stream.empty())
                    .map(eleIndexArr -> eleIndexArr[tupleId]).distinct().collect(Collectors.toList());
            final JavaPairRDD<FRddElementIndex, Long> tableIDColumnMap = tableIDColumnMapOp.get();
            final Map<FRddElementIndex, Long> eleId2colIdMap = FSparkUtils.localizeRDDMap(tableIDColumnMap, allElementIndexes);
            return new FPair<>(tableInfo, eleId2colIdMap);
        }).collect(
                // combine value if key is same
                HashMap::new,
                (map, pair) -> {
                    if (map.containsKey(pair._k)) {
                        final Map<FRddElementIndex, Long> storedValue = map.get(pair._k);
                        pair._v.forEach(storedValue::putIfAbsent);
                    } else {
                        map.put(pair._k, pair._v);
                    }
                },
                (m1, m2) -> {
                    m2.forEach((k, v) -> {
                        if (m1.containsKey(k)) {
                            final Map<FRddElementIndex, Long> storedValue = m1.get(k);
                            v.forEach(storedValue::putIfAbsent);
                        } else {
                            m1.put(k, v);
                        }
                    });
                }
        );
    }

    // getter setter

    public String getRule() {
        return rule;
    }

    public long getSupport() {
        return support;
    }

    public long getUnSupport() {
        return unSupport;
    }

    public long getRhsSupport() {
        return rhsSupport;
    }

    public long getRowSize() {
        return rowSize;
    }

    public FPositiveNegativeExample getSupportExamples() {
        return supportExamples;
    }

    public FPositiveNegativeExample getUnSupportExamples() {
        return unSupportExamples;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "rule='" + rule + '\'' +
                ", support=" + support +
                ", unSupport=" + unSupport +
                ", rhsSupport=" + rhsSupport +
                ", rowSize=" + rowSize +
                ", supportExamples=" + supportExamples +
                ", unSupportExamples=" + unSupportExamples +
                '}';
    }
}
