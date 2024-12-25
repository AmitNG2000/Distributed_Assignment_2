import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 *  Calculate N1 N2 N3
 * @pre there is file: <S3BucketName>/outputs/output_step1_word_count
 * @Input This step uses step1's output
 * @Input ((w1) or (w1 w2) or (w1 w2 w3), <LongWritable>))
 * @Output: ((w1,w2,w3) , (N1<LongWritable>,N2<LongWritable>,N3<LongWritable>, C0<0> ,C1<0> ,C2<0>)
 */
public class Step2CalculateN {
    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {

        // because the algorithm needs sort before it can continue, the map() do noting
        @Override
        public void map(Text words, LongWritable quantity, Context context) throws IOException,  InterruptedException {
            context.write(words, quantity);
        }
    }

    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text, LongWritable, Text, Text> {
        // mark 3-gram input as (w1, w2, w3)
        private long N1 = 0;  // Number of times w3 occurs
        private long N2 = 0;  // Number of times the sequence (w2, w3) occurs
        private long N3 = 0;  // Number of times the sequence (w1, w2, w3) occurs

        // The method accumulates the counts of N1, N2, and N3
        // If the key represents a 3-word n-gram, it emits N1, N2, N3 under the 3-gram key.
        @Override
        public void reduce(Text words, Iterable<LongWritable> quantities, Context context) throws IOException, InterruptedException {
            String[] wordsArr = words.toString().split(" ");

            //the input is one word
            if (wordsArr.length == 1) {
                for (LongWritable quantity : quantities) {
                    N1 += quantity.get();
                }

                //the input is two words
            } else if (wordsArr.length == 2) {
                for (LongWritable quantity : quantities) {
                    N2 += quantity.get();
                }

            } else if (wordsArr.length == 3) {

                for (LongWritable quantity : quantities) {
                    N3 += quantity.get();
                }

                // Emit (w1 w2 w3, N1 N2 N3) when we have 3-grams
                // <placeHolderForC> is in order to simulate an array. The C0 C1 C2 values will be calculated in step 3.
                // Output: ((w1,w2,w3) , (N1<LongWritable>,N2<LongWritable>,N3<LongWritable>, C0<0> ,C1<0> ,C2<0>)
                String placeHolderForC = (" " + Long.toString(0) + " " + Long.toString(0) + " " + Long.toString(0));
                context.write(words, new Text(N1 + " " + N2 + " " + N3 + placeHolderForC));

                // reset N1 N2 N3
                N1 = 0;
                N2 = 0;
                N3 = 0;
            } //end of if 3
        }

        //Partition by the first word
        public static class PartitionerClass extends Partitioner<Text, LongWritable> {
            @Override
            public int getPartition(Text words, LongWritable value, int numPartitions) {
                String[] wordsArr = words.toString().split(" ");
                return wordsArr[1].hashCode() % numPartitions;
            }
        }

        //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
            private long N1 = 0;  // Number of times w3 occurs
            private long N2 = 0;  // Number of times the sequence (w2, w3) occurs
            private long N3 = 0;  // Number of times the sequence (w1, w2, w3) occurs
            private long C0 = 0;  // Number of times w3 occurs
            private long C1 = 0;  // Number of times the sequence (w2, w3) occurs
            private long C2 = 0;  // Number of times the sequence (w1, w2, w3) occurs

            @Override
            public void reduce(Text words, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
                //TODO:combine the "arrays"
            }
        }

        public static void main(String[] args) throws Exception {
            System.out.println("[DEBUG] STEP 2 started!");
            System.out.println(args.length > 0 ? args[0] : "no args");
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Step 2: Calculate N");
            job.setJarByClass(Step2CalculateN.class);
            job.setMapperClass(MapperClass.class);
            job.setPartitionerClass(PartitionerClass.class);
            job.setCombinerClass(CombinerClass.class);
            job.setReducerClass(ReducerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

            FileInputFormat.addInputPath(job, new Path(String.format("%s/arbix.txt", App.s3Path)));
            FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step2_cal_N", App.s3Path)));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
