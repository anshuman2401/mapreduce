package com.mapreduce.application;

import com.mapreduce.utils.HBaseConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OptimizedHBaseDeleteJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: HBaseDeleteJob <ZOOKEEPER_QUORUM> <ZOOKEEPER_PORT> <hbase_table_name> <input_path> <output_path>");
            System.exit(-1);
        }

        String ZOOKEEPER_QUORUM = args[0];
        String ZOOKEEPER_PORT = args[1];
        String tableName = args[2];
        String inputPath = args[3];
        String outputPath = args[4];

        // Create configuration using the utility class
        Configuration config = HBaseConfigUtil.getSIConfiguration(ZOOKEEPER_QUORUM, ZOOKEEPER_PORT);
        config.set("hbase.table.name", tableName);
        config.set("mapreduce.job.queuename", "lowpriority");

        config.setInt("hbase.rpc.timeout", 60000);
        config.setInt("hbase.client.operation.timeout", 120000);

        Job job = Job.getInstance(config, "Optimized HBase Delete Job");
        job.setJarByClass(OptimizedHBaseDeleteJob.class);
        job.setMapperClass(HBaseDeleteMapper.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(0);

        int jobStatus = job.waitForCompletion(true) ? 0 : 1;

        long rowsDeleted = job.getCounters().findCounter("HBaseDeleteJob", "RowsDeleted").getValue();
        long rowsFailed = job.getCounters().findCounter("HBaseDeleteJob", "RowsFailed").getValue();

        System.out.println("Rows Deleted: " + rowsDeleted);
        System.out.println("Rows Failed: " + rowsFailed);
        return jobStatus;
    }

    public static class HBaseDeleteMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Table table;
        private List<Delete> deleteBatch;
        private static final int BATCH_SIZE = 50;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration config = context.getConfiguration();
            Connection connection = ConnectionFactory.createConnection(config);
            String tableName = config.get("hbase.table.name");
            table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName));
            deleteBatch = new ArrayList<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String rowKey = value.toString().trim();
            deleteBatch.add(new Delete(Bytes.toBytes(rowKey)));
            // Perform batched deletes
            if (deleteBatch.size() >= BATCH_SIZE) {
                flushDeletes(context);
            }
        }

        private void flushDeletes(Context context) {
            List<Delete> tempDeleteBatch = new ArrayList<>(deleteBatch);
            if (!tempDeleteBatch.isEmpty()) {
                try {
                    table.delete(tempDeleteBatch);
                    context.getCounter("HBaseDeleteJob", "RowsDeleted").increment(deleteBatch.size());
                } catch (Exception e) {
                    context.getCounter("HBaseDeleteJob", "RowsFailed").increment(deleteBatch.size());
                }

                deleteBatch.clear();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException {
            flushDeletes(context); // Ensure any remaining deletes are processed
            if (table != null) {
                table.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new OptimizedHBaseDeleteJob(), args);
        System.exit(exitCode);
    }
}

