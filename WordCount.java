import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.writable.tuple.PairWritable;
public class WordCount {
   public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
   {
      private Text word = new Text();
      
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
      {
        String tem = value.toString();
        System.out.printf("tem:%s\n",tem);
        String docId = tem.substring(0,10);
        tem = tem.substring(10,tem.length()-1);
      

        HashMap<String,Integer> wordCount =  new HashMap<>();
         StringTokenizer itr = new StringTokenizer(tem);
         while (itr.hasMoreTokens()) 
         {
           String curWord = standard(itr.nextToken());     
            wordCount.put(curWord,wordCount.getOrDefault(curWord,0)+1);
         }
        for (Map.Entry<String, Integer> set :
             wordCount.entrySet()) {
          String k = set.getKey();
          int v = set.getValue();
          word.set(k);
          PairWritable one = new PairWritable<Text,IntWritable>(new Text(docId),new IntWritable(v));
          context.write(word, one);
           System.out.printf("%s:%s:%d\n",k,docId,v);
        }
      }
   }
   
   public static class IntSumReducer extends Reducer<Text,PairWritable,Text,MapWritable> 
   {
      private MapWritable result = new MapWritable();
      public void reduce(Text key, Iterable<PairWritable<Text,IntWritable>> pairs, Context context) throws IOException, InterruptedException 
      {
         for (PairWritable<Text,IntWritable> pair : pairs) 
         {
          result.put(pair.getLeft(),pair.getRight());
          Text k = pair.getLeft();
          IntWritable v = pair.getRight();
         }
         context.write(key, result);
      }
   }

  public static String standard(String word){
      int n = word.length();
      int l = 0;
      int r = n-1;
      word = word.toLowerCase();
    while(l!=n && (word.charAt(l)>'z' || word.charAt(l)<'a'))
      l++;
    
    while(r!=-1 && (word.charAt(r)>'z' || word.charAt(r)<'a'))
      r--;
    if(r<l)
      return "";
    return word.substring(l, r+1);
  }
   
   public static void main(String[] args) throws Exception 
   {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "word count");
		
      job.setJarByClass(WordCount.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);
		
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}// WordCount

