package slowlime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
    public static void main(String[] args) throws Exception {
        var conf = new Configuration();
        var optParser = new GenericOptionsParser(conf, args);
        var remainingArgs = optParser.getRemainingArgs();

        if (remainingArgs.length != 3) {
            System.err.println("Usage: lab4 <input path> <temporary result path> <output path>");
            System.exit(2);
        }

        var groupJob = Job.getInstance(conf, "lab4: grouping");
        groupJob.setJarByClass(Main.class);
        groupJob.setMapperClass(GroupingMapper.class);
        groupJob.setCombinerClass(GroupingReducer.class);
        groupJob.setReducerClass(GroupingReducer.class);
        groupJob.setOutputKeyClass(Text.class);
        groupJob.setOutputValueClass(IntermediateRecord.class);
        FileInputFormat.addInputPath(groupJob, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(groupJob, new Path(remainingArgs[1]));

        var sortJob = Job.getInstance(conf, "lab4: sorting");
        sortJob.setJarByClass(Main.class);
        sortJob.setMapperClass(SortingMapper.class);
        sortJob.setOutputKeyClass(OutputKey.class);
        sortJob.setOutputValueClass(IntermediateRecord.class);
        FileInputFormat.addInputPath(sortJob, new Path(remainingArgs[1]));
        FileOutputFormat.setOutputPath(sortJob, new Path(remainingArgs[2]));

        var success = groupJob.waitForCompletion(true) && sortJob.waitForCompletion(true);

        System.exit(success ? 0 : 1);
    }
}
