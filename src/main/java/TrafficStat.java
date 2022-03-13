import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


class PhoneTraffic implements Writable {
    private long up;
    private long down;
    private long sum;

    public long getUp() {
        return up;
    }

    public long getDown() {
        return down;
    }

    public long getSum() {
        return sum;
    }

    public PhoneTraffic(long up, long down, long sum) {
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.sum = dataInput.readLong();
    }
}

class TrafficStat {
    public static class TrafficMapper extends Mapper<Object, Text, Text, PhoneTraffic> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\t");
            if (lines.length < 10) {
                return;
            }

            String phone = lines[1];
            try {
                long up = Long.parseLong(lines[8]);
                long down = Long.parseLong(lines[9]);
                context.write(new Text(phone), new PhoneTraffic(up, down, up + down));
            } catch (NumberFormatException e) {
                System.err.println("Parse long failed" + e.getMessage());
            }
        }
    }

    public static class TrafficReducer extends Reducer<Text, PhoneTraffic, Text, PhoneTraffic> {
        public void reduce(Text key, Iterable<PhoneTraffic> values, Context context) throws IOException, InterruptedException {
            int totalUp = 0;
            int totalDown = 0;
            int sumTraffic = 0;
            for (PhoneTraffic val : values) {
                totalUp += val.getUp();
                totalDown += val.getDown();
                sumTraffic += val.getSum();
            }
            context.write(key, new PhoneTraffic(totalUp, totalDown, sumTraffic));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage TrafficStat <int> <out>");
            System.exit(2);
        }
        System.out.println("otherArgs: " + Arrays.toString(otherArgs));
        Job job = Job.getInstance(conf, "TrafficStat");

        job.setJarByClass(TrafficStat.class);
        job.setMapperClass(TrafficMapper.class);
        job.setCombinerClass(TrafficReducer.class);
        job.setReducerClass(TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneTraffic.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
