import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
        public void map(LongWritable key, Text sentence, Context context) throws IOException, InterruptedException {
            // Tokenize the input sentence
            StringTokenizer tokenizer = new StringTokenizer(sentence.toString());

            // Track words for constructing n-grams
            String[] words = new String[3];
            int count = 0;

            while (tokenizer.hasMoreTokens()) {
                // Get the next token (word)
                String token = tokenizer.nextToken().trim();

                // Filter out unwanted characters or garbage:
                // 1. Skip empty or null tokens
                if (token == null || token.isEmpty()) {
                    continue;
                }

                // 2. Skip tokens with non-Hebrew or invalid characters
                if (!isValidToken(token)) {
                    continue;
                }

                // 3. Check if the word is a stop word
                if (App.stopWords.contains(token)) {
                    continue; // Skip this word if it's a stop word
                }

                // Print the cleaned token for debugging
                System.out.println("Processed Token: " + token);

                // Shift words to construct n-grams
                words[count % 3] = token;
                count++;

                if (count >= 1) {
                    // Emit unigram (w1)
                    w1.set(words[(count - 1) % 3]);
                    wordsOutput.set(w1);
                    context.write(wordsOutput, one);
                    context.write(emptyText, one);
                }

                if (count >= 2) {
                    // Emit bigram (w1, w2)
                    w1.set(words[(count - 2) % 3]);
                    w2.set(words[(count - 1) % 3]);
                    wordsOutput.set(w1.toString() + " " + w2.toString());
                    context.write(wordsOutput, one);
                }

                if (count >= 3) {
                    // Emit trigram (w1, w2, w3)
                    w1.set(words[(count - 3) % 3]);
                    w2.set(words[(count - 2) % 3]);
                    w3.set(words[(count - 1) % 3]);
                    wordsOutput.set(w1.toString() + " " + w2.toString() + " " + w3.toString());
                    context.write(wordsOutput, one);
                }
            }
        }

        // Helper method to validate tokens
        private boolean isValidToken(String token) {
            // Check if the token contains only valid characters (e.g., Hebrew letters and spaces)
            return token.matches("[א-ת]+");
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

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //For demo testing
        //FileInputFormat.addInputPath(job, new Path(String.format("%s/arbix.txt" , App.s3Path)));
        //3-Gram input
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step1_word_count" , App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
