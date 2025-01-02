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

public class HBaseDeleteJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: HBaseDeleteJob <ZOOKEEPER_QUORUM> <ZOOKEEPER_PORT> <hbase_table_name> <input_path> " +
                    "<output_path>");
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

        Job job = Job.getInstance(config, "HBase Delete Job");
        job.setJarByClass(HBaseDeleteJob.class);
        job.setMapperClass(HBaseDeleteMapper.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class HBaseDeleteMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Table table;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration config = context.getConfiguration();
            Connection connection = ConnectionFactory.createConnection(config);
            String tableName = config.get("hbase.table.name");
            table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String rowKey = value.toString().trim();
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            try {
                table.delete(delete);
                context.write(new Text(rowKey), new Text("Deleted"));
            }catch(Exception e) {
                context.write(new Text("Failed to delete row: "), new Text(rowKey));
                context.write(new Text("Error: "), new Text(e.getMessage()));
                throw e;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException {
            if (table != null) {
                table.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseDeleteJob(), args);
        System.exit(exitCode);
    }
}

