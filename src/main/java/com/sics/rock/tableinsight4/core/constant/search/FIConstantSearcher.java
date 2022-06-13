package com.sics.rock.tableinsight4.core.constant.search;

import com.sics.rock.tableinsight4.core.constant.FConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;

import java.util.List;

public interface FIConstantSearcher {

    List<FConstantInfo> search(FTableDatasetMap tableDatasetMap);

}
