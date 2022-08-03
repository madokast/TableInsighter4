package com.sics.rock.tableinsight4.preprocessing.constant.search;

import com.sics.rock.tableinsight4.preprocessing.constant.FConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;

import java.util.List;

/**
 * Search constants by ratio and return as FConstantInfo
 * Generate constants from FConstantConfig in columnInfo and return as FConstantInfo
 *
 * All FConstantInfo convert to FConstant and stored in columnInfo
 */
public interface FIConstantSearcher {

    List<FConstantInfo> search(FTableDatasetMap tableDatasetMap);

}
