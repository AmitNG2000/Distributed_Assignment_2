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


/**
 * Calculate N1 N2 N3
 *
 * @pre there is file: <S3BucketName>/outputs/output_step1_word_count
 * @Input This step uses step1's output
 * @Input (( w1) or (w1 w2) or (w1 w2 w3), <LongWritable>))
 * @Output: (( w1, w2, w3) , Text(N1<LongWritable>,N2<LongWritable>,N3<LongWritable>, C0<0> ,C1<0> ,C2<0>)
 */
public class Step2CalculateN {
    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    //public class Mapper<lineId,line,words,quantity>
    //Example of a line form step 1 output: "w1 w1 w3	1"
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text emptyText = new Text();

        // reverse the order of the words to get the proper order at sorting
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            String[] keyAndValue = line.toString().split("\t");
            Text words = new Text(keyAndValue[0]);
            if (words.equals(emptyText)) return; //handled in step 3.
            LongWritable quantity = new LongWritable(Long.parseLong(keyAndValue[1]));

            Text reversedWordsText = UtilsFunctions.reverseTextWithSpaces(words);
            context.write(reversedWordsText, quantity);
        }
    }

    //Partition by the first word
    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text words, LongWritable value, int numPartitions) {
            String[] wordsArr = words.toString().split(" ");
            return wordsArr[0].hashCode() % numPartitions;
        }
    }


    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    //Input Example:
    public static class ReducerClass extends Reducer<Text, LongWritable, Text, Text> {
        // We will mark 3-gram input as (w1, w2, w3)
        private long N1 = 0;  // Number of times w3 occurs
        private long N2 = 0;  // Number of times the sequence (w2, w3) occurs
        private long N3 = 0;  // Number of times the sequence (w1, w2, w3) occurs

        private final Text lastSingedWord = new Text(""); //just the pointer is final
        private final Text lastPairWord = new Text("");
        private final Text lastTripleWord = new Text("");

        private final String placeHolderForC = " 0 0 0"; //placeholder for the values of C0,C1,C2 that will be calculated in step3


        // The method accumulates the counts of N1, N2, and N3
        // If the key represents a 3-word n-gram, it emits N1, N2, N3 under the 3-gram key.
        @Override
        public void reduce(Text words, Iterable<LongWritable> quantities, Context context) throws IOException, InterruptedException {
            String[] wordsArr = words.toString().split(" ");

            //the input is one word
            if (wordsArr.length == 1) {

                if (!lastSingedWord.equals(words)) {
                    lastSingedWord.set(words);
                    N1 = 0;
                }
                for (LongWritable quantity : quantities) {
                    N1 += quantity.get();
                }

            //the input is two words
            } else if (wordsArr.length == 2) {

                if (!lastPairWord.equals(words)) {
                    lastPairWord.set(words);
                    N2 = 0;
                }
                for (LongWritable quantity : quantities) {
                    N2 += quantity.get();
                }

            //the input is three  words
            } else if (wordsArr.length == 3) {

                for (LongWritable quantity : quantities) {
                    N3 += quantity.get();
                }

                //emit and reset N3
                Text originalWords = UtilsFunctions.reverseTextWithSpaces(words); //reverse the reverse in the map()
                context.write(originalWords, new Text(N1 + " " + N2 + " " + N3 + placeHolderForC));
                lastTripleWord.set(words);
                N3 = 0;
            } //end of if 3

        } //end of reduce()
    } //end of reducer class


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 2: Calculate N");
        job.setJarByClass(Step2CalculateN.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        // job.setCombinerClass(Step1WordCount.ReducerClass.class); //Don't think it will do a differnt
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step1_word_count", App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step2_cal_N", App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    } // end of main
}
