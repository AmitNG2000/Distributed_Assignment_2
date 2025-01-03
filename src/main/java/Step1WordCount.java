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
 *  Calculate the number single (w1), pairs (w1,w2) and trio (w1,w2,w3) in the corpus.
 * @pre for demo, the inoutfile is at S3 with App.<bucketName>
 * @Input split from a text file
 * @Output: ((w1), <LongWritable>), ((w1,w2), <LongWritable>), ((w1,w2,w3), <LongWritable>)
 */
public class Step1WordCount {
    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text w1 = new Text(); //Warning: just the pointer is final
        private final Text w2 = new Text();
        private final Text w3 = new Text();
        private final Text wordsOutput = new Text(); // Reuse output Text object
        private final Text emptyText = new Text();


        @Override
        public void map(LongWritable key, Text sentence, Context context) throws IOException,  InterruptedException {
            // Tokenize the input sentence
            StringTokenizer tokenizer = new StringTokenizer(sentence.toString());

            // Store the words for constructing n-grams
            String[] words = new String[3];
            int count = 0;

            while (tokenizer.hasMoreTokens()) {
                // Shift words to construct n-grams
                words[count % 3] = tokenizer.nextToken();
                count++;

                if (count >= 1) {
                    // Emit uni-gram (w1)
                    w1.set(words[(count - 1) % 3]);
                    wordsOutput.set(w1);
                    context.write(wordsOutput, one);
                    context.write(emptyText, one);
                }

                if (count >= 2) {
                    // Emit bi-gram (w1, w2)
                    w1.set(words[(count - 2) % 3]);
                    w2.set(words[(count - 1) % 3]);
                    wordsOutput.set(w1.toString() + " " + w2.toString());
                    context.write(wordsOutput, one);
                }

                if (count >= 3) {
                    // Emit tri-gram (w1, w2, w3)
                    w1.set(words[(count - 3) % 3]);
                    w2.set(words[(count - 2) % 3]);
                    w3.set(words[(count - 1) % 3]);
                    wordsOutput.set(w1.toString() + " " + w2.toString() + " " + w3.toString());
                    context.write(wordsOutput, one);
                }
            } //end of while
        }
    }

    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1: Word Count");
        job.setJarByClass(Step1WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path(String.format("%s/arbix.txt" , App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step1_word_count" , App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
