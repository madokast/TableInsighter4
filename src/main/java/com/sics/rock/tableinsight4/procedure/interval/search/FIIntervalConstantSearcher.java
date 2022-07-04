package com.sics.rock.tableinsight4.procedure.interval.search;


import com.sics.rock.tableinsight4.procedure.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;

import java.util.List;

public interface FIIntervalConstantSearcher {

    List<FIntervalConstantInfo> search(FTableDatasetMap tableDatasetMap);

}
