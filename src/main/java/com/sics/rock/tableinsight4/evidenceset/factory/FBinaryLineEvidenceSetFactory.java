package com.sics.rock.tableinsight4.evidenceset.factory;

import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FLocalPLI;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Binary-line evidence set factory
 */
public class FBinaryLineEvidenceSetFactory implements Serializable {

    private final SparkSession spark;

    private final JavaSparkContext sc;

    private final int PLIBroadcastSizeMB;


    public FIEvidenceSet createSingleTableBinaryLineEvidenceSet(
            FTableInfo tableInfo, FPLI PLI, FPredicateIndexer predicates, long tableLength) {
        return create(tableInfo, tableInfo, PLI, predicates, tableLength, tableLength, true);
    }

    public FIEvidenceSet createBinaryTableBinaryLineEvidenceSet(
            FTableInfo leftTableInfo, FTableInfo rightTableInfo, FPLI PLI,
            FPredicateIndexer predicates, long leftTableLength, long rightTableLength) {
        return create(leftTableInfo, rightTableInfo, PLI, predicates, leftTableLength, rightTableLength, false);
    }

    private FIEvidenceSet create(
            FTableInfo leftTableInfo, FTableInfo rightTableInfo, FPLI PLI,
            FPredicateIndexer predicates, long leftTableLength, long rightTableLength, boolean sameTableFlag) {
        final Broadcast<FPredicateIndexer> predicatesBroadcast = sc.broadcast(predicates);
        final JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> leftPLI = PLI.getTablePLI(leftTableInfo);
        final JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> rightPLI = PLI.getTablePLI(rightTableInfo);

        // a list holds ES in section. The binary-line ES is constructed in cartesian product.
        // However the cartesian product between RDDs conducted by spark is OOM inclined.
        // A manual cartesian product is coded in final
        final List<JavaPairRDD<FBitSet, Long>> ESSectionList = new ArrayList<>();

        // batch size of PLI in each broadcasting
        final int batchSizeMB = PLIBroadcastSizeMB * Runtime.getRuntime().availableProcessors();

        return null;
    }

    public FBinaryLineEvidenceSetFactory(SparkSession spark, int PLIBroadcastSizeMB) {
        this.spark = spark;
        this.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        this.PLIBroadcastSizeMB = PLIBroadcastSizeMB;
    }
}
