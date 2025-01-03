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
 * Calculate the probability P
 *
 * @pre there is files: <S3BucketName>/outputs/output_step2_cal_N, <S3BucketName>/outputs/output_step3_cal_C
 * @Input step2's and Step3's output
 * @Input (Text(w1 w2 w3), Text(N1 N2 N3 C0 C1 C2)
 * @Output: (Text(w1, w2, w3) , FloatWritable P)
 */
public class Step4CalculateP {

    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    //public class Mapper<lineId,line,words,quantity>
    //Example of a line form step 1 output: "w1 w2 w3	1" as the record reader import it.
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text emptyText = new Text(); //Note: just the pointer is final, the value itself is chainable.
        /**
         * Parse the RR output
         * Reverse the order of the words to get the proper order at sorting
         * If the word is a single word, emit (Text emptyText, LongWritable one)
         * @param (lineId, line)
         */
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            //Parse the RR output
            String[] keyAndValue = line.toString().split("\t");
            Text wordsText = new Text(keyAndValue[0]);
            String[] wordsArr = keyAndValue[0].split(" ");
            LongWritable quantity = new LongWritable(Long.parseLong(keyAndValue[1]));

            if (wordsText.equals(emptyText)){
                context.write(wordsText, quantity);

                //emit the word and an empty text in order to calculate C0
            } else if (wordsArr.length == 1) {
                context.write(wordsText, quantity);

                // switch the order of the first two words and emit
            } else if (wordsArr.length == 2) {
                Text reversedWordsText = UtilsFunctions.reverseTextWithSpaces(wordsText);
                context.write(reversedWordsText,quantity);

                // switch the order of the first two words and emit
            } else if (wordsArr.length == 3) {
                Text firstTwoWords = new Text (wordsArr[0] + " " + wordsArr[1]);
                String reversedFirstTwoWords  = UtilsFunctions.reverseTextWithSpaces(firstTwoWords).toString();
                Text outputTextKey = new Text ( reversedFirstTwoWords + " " + wordsArr[2]);
                context.write(outputTextKey, quantity);
            }

        } //end of  map()
    } //end of MapperClass

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
        private long C0 = 0;  // The total number of word instances in the corpus.
        private long C1 = 0;  // The number of times w2 occurs.
        private long C2 = 0;  // The number of times sequence (w1,w2) occurs.

        private final Text lastSingedWord = new Text(""); //just the pointer is final
        private final Text lastPairWord = new Text("");

        private final String placeHolderForN = "0 0 0"; //placeholder for the values of C0,C1,C2 that will be calculated in step3
        private final Text emptyText = new Text(); //Note: just the pointer is final, the value itself is chainable.


        // The method accumulates the counts of C0, C1, C2
        // If the key represents a 3-word n-gram, it emits C0, C1, C2 under the 3-gram key.
        @Override
        public void reduce(Text words, Iterable<LongWritable> quantities, Context context) throws IOException, InterruptedException {

            // Calculate C0
            if (words.equals(emptyText)) {
                for (LongWritable quantity : quantities) {
                    C0 += quantity.get();
                }
                return;
            }

            String[] wordsArr = words.toString().split(" ");

            //the input is one word
            if (wordsArr.length == 1) {

                if (!lastSingedWord.equals(words)) {
                    lastSingedWord.set(words);
                    C1 = 0;
                }
                for (LongWritable quantity : quantities) {
                    C1 += quantity.get();
                }

                //the input is two words
            } else if (wordsArr.length == 2) {

                if (!lastPairWord.equals(words)) {
                    lastPairWord.set(words);
                    C2 = 0;
                }
                for (LongWritable quantity : quantities) {
                    C2 += quantity.get();
                }

                //emit under the original 3-words key
            } else if (wordsArr.length == 3) {
                Text firstTwoWords = new Text (wordsArr[0] + " " + wordsArr[1]);
                String originalFirstTwoWords  = UtilsFunctions.reverseTextWithSpaces(firstTwoWords).toString();
                Text originalWords = new Text ( originalFirstTwoWords + " " + wordsArr[2]);
                context.write(originalWords, new Text(placeHolderForN + " " + C0 + " " + C1 + " " + C2));
            } //end of if 3

        } //end of reduce()
    } //end of reducer class


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 3: Calculate C");
        job.setJarByClass(Step3CalculateC.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        // job.setCombinerClass(Step1WordCount.ReducerClass.class); //Don't think it will do a different
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant one
        // job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        // TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step1_word_count", App.s3Path)));
        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step1_word_count", App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step3_cal_C", App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    } // end of main
}

