package com.mapreduce.application;

import org.apache.hadoop.util.ToolRunner;

public class HBaseJobRunner {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: HBaseJobRunner <job-name> [job-arguments]");
            System.err.println("Available jobs:");
            System.err.println("  HBaseToHDFS <table_name> <output_path> <filter_string>");
            System.err.println("  HBaseDeleteJob <input_path> <hbase_table_name>");
            System.exit(-1);
        }

        String jobName = args[0];
        String[] jobArgs = new String[args.length - 1];
        System.arraycopy(args, 1, jobArgs, 0, args.length - 1);

        int exitCode;
        switch (jobName) {
            case "HBaseToHDFS":
                exitCode = ToolRunner.run(new HBaseToHDFS(), jobArgs);
                break;
            case "HBaseDeleteJob":
                exitCode = ToolRunner.run(new HBaseDeleteJob(), jobArgs);
                break;
            default:
                System.err.println("Unknown job: " + jobName);
                System.exit(-1);
                return;
        }

        System.exit(exitCode);
    }
}

