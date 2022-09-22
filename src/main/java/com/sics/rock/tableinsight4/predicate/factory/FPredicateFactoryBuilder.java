package com.sics.rock.tableinsight4.predicate.factory;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.factory.impl.FBinaryTableCrossLinePredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.impl.FSingleLinePredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.impl.FSingleTableCrossLinePredicateFactory;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;

import java.util.List;

/**
 * A predicate-factory builder with 2-step construction dividing materials and configs.
 *
 * @author zhaorx
 */
public class FPredicateFactoryBuilder implements FTiEnvironment {

    private final FDerivedColumnNameHandler derivedColumnNameHandler;

    public FSingleLinePredicateFactoryBase buildForSingleLinePredicate() {
        return new FSingleLinePredicateFactoryBase();
    }

    public FSingleTableCrossLinePredicateFactoryBase buildForSingleTableCrossLinePredicate() {
        return new FSingleTableCrossLinePredicateFactoryBase();
    }

    public FBinaryTableCrossLinePredicateFactoryBase buildForBinaryTableCrossLinePredicate(FTableDatasetMap datasetMap, FPLI PLI) {
        return new FBinaryTableCrossLinePredicateFactoryBase(datasetMap, PLI);
    }

    public FPredicateFactoryBuilder(final FDerivedColumnNameHandler derivedColumnNameHandler) {
        this.derivedColumnNameHandler = derivedColumnNameHandler;
    }

    /*============================ inner classes =====================================*/

    public class FSingleLinePredicateFactoryBase {
        public FIPredicateFactory use(FTableInfo tableInfo, List<FExternalPredicateInfo> otherInfos) {
            return new FSingleLinePredicateFactory(tableInfo, otherInfos, derivedColumnNameHandler);
        }
    }

    public class FSingleTableCrossLinePredicateFactoryBase {
        public FIPredicateFactory use(FTableInfo tableInfo, List<FExternalPredicateInfo> otherInfos) {
            return new FSingleTableCrossLinePredicateFactory(tableInfo, otherInfos, derivedColumnNameHandler,
                    config().constPredicateCrossLine);
        }
    }

    public class FBinaryTableCrossLinePredicateFactoryBase {

        private final FTableDatasetMap datasetMap;

        private final FPLI PLI;

        private FBinaryTableCrossLinePredicateFactoryBase(final FTableDatasetMap datasetMap, final FPLI PLI) {
            this.datasetMap = datasetMap;
            this.PLI = PLI;
        }

        public FIPredicateFactory use(FTableInfo leftTableInfo, FTableInfo rightTableInfo, List<FExternalPredicateInfo> otherInfos) {
            return new FBinaryTableCrossLinePredicateFactory(leftTableInfo, rightTableInfo, otherInfos,
                    derivedColumnNameHandler, datasetMap, PLI, config().crossColumnThreshold);
        }
    }

}
