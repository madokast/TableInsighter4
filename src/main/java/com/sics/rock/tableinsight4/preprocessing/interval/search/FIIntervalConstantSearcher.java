package com.sics.rock.tableinsight4.preprocessing.interval.search;


import com.sics.rock.tableinsight4.preprocessing.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;

import java.util.List;

/**
 * interval constant searcher
 *
 * @author zhaorx
 */
public interface FIIntervalConstantSearcher {

    List<FIntervalConstantInfo> search(FTableDatasetMap tableDatasetMap);

}
