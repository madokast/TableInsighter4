package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.env.FTiEnvironment;

public class FEvidenceSetFactoryBuilder implements FTiEnvironment {

    public FSingleLineEvidenceSetFactory buildSingleLineEvidenceSetFactory() {
        final FTiConfig config = config();
        return new FSingleLineEvidenceSetFactory(spark(), config.evidenceSetPartitionNumber,
                config.positiveNegativeExampleSwitch, config.positiveNegativeExampleNumber);
    }

    public FBinaryLineEvidenceSetFactory buildBinaryLineEvidenceSetFactory(boolean sameTableFlag) {
        final FTiConfig config = config();
        return new FBinaryLineEvidenceSetFactory(spark(), sameTableFlag);
    }


}
