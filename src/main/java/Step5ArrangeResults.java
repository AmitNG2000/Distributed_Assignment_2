import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Calculate the probability P
 *
 * @pre there is files: <S3BucketName>/outputs/output_step4_cal_p
 * @Input step4's output
 * @Input (Text(w1 w2 w3), Text(p)
 * @Output: (Text(w1 w2 w3) , FloatWritable P)
 */
public class Step5ArrangeResults {

    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    //public class Mapper<lineId,line>
    //Example of a line form step 4 output: (w1 w2 w3 , 0.7)
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * Parse the RR output and emit
         * @param (lineId, line)
         */
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            //Parse the RR output
            String[] keyAndValue = line.toString().split("\t");
            String[] words = keyAndValue[0].split(" ");
            float p = Float.parseFloat(keyAndValue[1]);

            Text newKey = new Text(words[0] + " " +  words[1] + " " + (1-p));

            context.write(newKey , new Text(words[2]));
        } //end of  map()
    } //end of MapperClass

    //Default Partition
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text, Text, Text, FloatWritable> {

        /**
         * Reverse the change made in the mapper. Making the wanted look.
         *
         * @Input (Text (w1 w2 {1-P}), Text(w3))
         * @Output (Text(w1 w2 w3), Text P)
         */
        @Override
        public void reduce(Text key, Iterable<Text> words3, Context context) throws IOException, InterruptedException {
            for (Text w3 : words3) {
                //float p = Float.parseFloat(probability.toString());

                String[] keyArr = key.toString().split(" ");
                Text originalKey = new Text(keyArr[0] + " " + keyArr[1] + " " + w3.toString());
                FloatWritable probability = new FloatWritable(1 - Float.parseFloat(keyArr[2]));

                context.write(originalKey, probability);
            }

        } //end of reduce()
    } //end of reducer class


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 5: Arrange Results");
        job.setJarByClass(Step5ArrangeResults.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        // job.setCombinerClass(Step1WordCount.ReducerClass.class); //Don't think it will do a different
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step4_cal_P", App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step5_final_results", App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    } // end of main
}


