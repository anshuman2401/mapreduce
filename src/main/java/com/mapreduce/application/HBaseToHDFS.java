package com.mapreduce.application;

import com.mapreduce.utils.HBaseConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseToHDFS extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: HBaseToHDFS <ZOOKEEPER_QUORUM> <ZOOKEEPER_PORT> <table_name> <output_path> <filter_string>");
            System.exit(-1);
        }

        String ZOOKEEPER_QUORUM = args[0];
        String ZOOKEEPER_PORT = args[1];
        String tableName = args[2];
        String outputPath = args[3];
        String filterString = args[4];

        // Create configuration using the utility class
        Configuration config = HBaseConfigUtil.getSIConfiguration(ZOOKEEPER_QUORUM, ZOOKEEPER_PORT);

        // Pass filter string to the configuration
        config.set("filterString", filterString);

        Job job = Job.getInstance(config, "HBase Keys Filter to HDFS");
        job.setJarByClass(HBaseToHDFS.class);

        // Set up HBase Scan
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                HBaseKeyMapper.class,
                Text.class,
                Text.class,
                job
        );

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Submit job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new HBaseToHDFS(), args);
        System.exit(exitCode);
    }

    public static class HBaseKeyMapper extends TableMapper<Text, Text> {
        private String filterString;

        @Override
        protected void setup(Context context) {
            filterString = context.getConfiguration().get("filterString");
        }

        @Override
        public void map(ImmutableBytesWritable key, org.apache.hadoop.hbase.client.Result value, Context context) throws IOException, InterruptedException {
            String keyString = Bytes.toString(key.get());
            if (keyString.contains(filterString)) {
                context.write(new Text(keyString), new Text());
            }
        }
    }
}

