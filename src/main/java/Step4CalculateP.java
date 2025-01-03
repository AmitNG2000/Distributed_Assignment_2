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
 * @pre there is files: <S3BucketName>/outputs/output_step2_cal_N, <S3BucketName>/outputs/output_step3_cal_C
 * @Input step2's and Step3's output
 * @Input (Text(w1 w2 w3), Text(N1 N2 N3 C0 C1 C2)
 * @Output: (Text(w1, w2, w3) , FloatWritable P)
 */
public class Step4CalculateP {

    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    //public class Mapper<lineId,line>
    //Example of a line form step 3 output: (w1 w2 w3 , N1 N2 N3 C0 C1 C2)
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * Parse the RR output and emit
         * @param (lineId, line)
         */
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            //Parse the RR output
            String[] keyAndValue = line.toString().split("\t");
            context.write(new Text(keyAndValue[0]) , new Text(keyAndValue[1]));
        } //end of  map()
    } //end of MapperClass

    //Default Partition
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
    public static class Combiner extends Reducer<Text, Text, Text, FloatWritable> {
        // TODO:
    }


        //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text, Text, Text, FloatWritable> {


        /**
         * Accumulates the counts of N1 N2 N3 C0 C1 C2
         * Calculate the probability P and emit
         *
         * @Input (Text(w1 w2 w3), Text(N1 N2 N3 C0 C1 C2)
         * @Output (Text(w1 w2 w3), FloatWritable P)
         */
        @Override
        public void reduce(Text words, Iterable<Text> valuesNC, Context context) throws IOException, InterruptedException {
            double N1 = 0;  // Number of times w3 occurs
            double N2 = 0;  // Number of times the sequence (w2, w3) occurs
            double N3 = 0;  // Number of times the sequence (w1, w2, w3) occurs
            double C0 = 0;  // The total number of word instances in the corpus
            double C1 = 0;  // The number of times w2 occurs
            double C2 = 0;  // The number of times the sequence (w1, w2) occurs

            // Accumulate the counts from the values
            for (Text valueNC : valuesNC) {
                String[] counts = valueNC.toString().split(" ");
                N1 += Double.parseDouble(counts[0]);
                N2 += Double.parseDouble(counts[1]);
                N3 += Double.parseDouble(counts[2]);
                C0 += Double.parseDouble(counts[3]);
                C1 += Double.parseDouble(counts[4]);
                C2 += Double.parseDouble(counts[5]);
            }

            //calculate the probability
            double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
            double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);

            double p = k3 * N3/C2 + (1 - k3) * k2 * N2/C1 + (1 - k3) * (1 - k2) * N1/C0;

            // Emit the result
            context.write(words, new FloatWritable((float) p));

        } //end of reduce()
    } //end of reducer class


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 4: Calculate C");
        job.setJarByClass(Step4CalculateP.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        // job.setCombinerClass(Step1WordCount.ReducerClass.class); //Don't think it will do a different
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant one
        // job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step2_cal_N", App.s3Path)));
        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step3_cal_C", App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step4_cal_P", App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    } // end of main
}

