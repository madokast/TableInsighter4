package com.sics.rock.tableinsight4.core.range.search;


import com.sics.rock.tableinsight4.core.range.FColumnValueRange;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;

import java.util.List;

public interface FIRangeClusterSearcher {

    List<FColumnValueRange> search(FTableDatasetMap tableDatasetMap);

}
