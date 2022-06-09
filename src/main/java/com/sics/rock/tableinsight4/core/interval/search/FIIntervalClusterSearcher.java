package com.sics.rock.tableinsight4.core.interval.search;


import com.sics.rock.tableinsight4.core.interval.FIntervalConstant;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;

import java.util.List;

public interface FIIntervalClusterSearcher {

    List<FIntervalConstant> search(FTableDatasetMap tableDatasetMap);

}
