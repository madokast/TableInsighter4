package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.env.FTiEnvironment;

/**
 * PLI constructor factory
 *
 * @author zhaorx
 */
public class FPliConstructorFactory implements FTiEnvironment {

    public FPliConstructor create() {
        final FTiConfig config = config();
        return new FPliConstructor(config.idColumnName, config.sliceLengthForPLI,
                config.positiveNegativeExampleSwitch, spark());
    }

}
