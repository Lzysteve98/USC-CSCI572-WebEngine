import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unigram {

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Text docId = new Text();
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] doc = value.toString().split("\t", 2);
      docId.set(doc[0]);
      StringTokenizer itr = new StringTokenizer(doc[1].toLowerCase().replaceAll("[^a-z]+", " "));
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, docId);
      }
    }
  }

  public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text word, Iterable<Text> docIds, Context context) throws IOException, InterruptedException {
      HashMap<String, Integer> freqMap = new HashMap<String, Integer>();

      for (Text docId : docIds) {
          freqMap.put(docId.toString(), freqMap.getOrDefault(docId.toString(), 0) + 1);
      }
      
      StringBuilder frequencyList = new StringBuilder();
      for (String docId : freqMap.keySet()) {
        frequencyList.append(docId).append(":").append(freqMap.get(docId)).append(" ");
      }
      result.set(frequencyList.toString().trim());
      context.write(word, result);
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(Unigram.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }
}