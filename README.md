# HBase MapReduce Jobs

## Filtering Row Keys with a Substring
This job allows filtering of row keys in an HBase table based on a given substring.

### Arguments
- **Job Name:** `HBaseToHDFS`
- **ZOOKEEPER_QUOROM:** `zookeeper0-az-prod-ci,zookeeper1-az-prod-ci,zookeeper2-az-prod-ci`
- **ZOOKEEPER_PORT:** `2181`
- **Table Name:** `test-table`
- **Output File:** `/test/output2`
- **Filter String:** `row`

### Command
```bash
hadoop jar hbase-mapreduce-job-1.0-SNAPSHOT.jar \
  "HBaseToHDFS" \
  "zookeeper0-az-prod-ci,zookeeper1-az-prod-ci,zookeeper2-az-prod-ci" \
  "2181" \
  "test-table" \
  "/test/output2" \
  "row"
```

## Deleting Row Keys via File
This job enables deleting row keys in an HBase table using a file that contains the row keys to be deleted.

### Arguments
- **Job Name:** `HBaseDeleteJob`
- **ZOOKEEPER_QUOROM:** `zookeeper0-az-prod-ci,zookeeper1-az-prod-ci,zookeeper2-az-prod-ci`
- **ZOOKEEPER_PORT:** `2181`
- **Table Name:** `test-table`
- **Input File:** `/test/output2/part-r-00000`
- **Output String:** `/test/output2/results`

### Command
```bash
hadoop jar hbase-mapreduce-job-1.0-SNAPSHOT.jar \
  "HBaseDeleteJob" \
  "zookeeper0-az-prod-ci,zookeeper1-az-prod-ci,zookeeper2-az-prod-ci" \
  "2181" \
  "test-table" \
  "/test/output2/part-r-00000" \
  "/test/output2/results"
```

## Deleting Row Keys in batch via File
This job enables deleting row keys in an HBase table using a file that contains the row keys to be deleted in batches.

### Arguments
- **Job Name:** `OptimizedHBaseToHDFS`
- **ZOOKEEPER_QUOROM:** `zookeeper0-az-prod-ci,zookeeper1-az-prod-ci,zookeeper2-az-prod-ci`
- **ZOOKEEPER_PORT:** `2181`
- **Table Name:** `test-table`
- **Input File:** `/test/output2/part-r-00000`
- **Output String:** `/test/output2/results`

### Command
```bash
hadoop jar hbase-mapreduce-job-1.0-SNAPSHOT.jar \
  "OptimizedHBaseToHDFS" \
  "zookeeper0-az-prod-ci,zookeeper1-az-prod-ci,zookeeper2-az-prod-ci" \
  "2181" \
  "test-table" \
  "/test/output2/part-r-00000" \
  "/test/output2/results"
```

## Notes
- Ensure that the Zookeeper quorum and ports are correctly configured for your environment.
- Verify the existence and accessibility of input and output file paths before executing the commands.
- Use the appropriate JAR file version matching your HBase setup.
