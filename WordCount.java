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
           if(isNum(curWord))
             continue;
            wordCount.put(curWord,wordCount.getOrDefault(curWord,0)+1);
           System.out.println(curWord);
         }
        for (Map.Entry<String, Integer> set :
             wordCount.entrySet()) {
          String k = set.getKey();
          int v = set.getValue();
          word.set(k);
          Text one = new Text(String.format("%s:%s",docId,String.valueOf(v)));
          context.write(word, one);
        }
      }
   }
   
   public static class IntSumReducer extends Reducer<Text,Text,Text,Text> 
   {
      public void reduce(Text key, Iterable<Text> texts, Context context) throws IOException, InterruptedException 
      {
        StringBuilder ans = new StringBuilder("");
         for (Text text : texts) 
         {
           ans.append(" ");
           ans.append(text.toString());
         }
        // ans.deleteCharAt(ans.length()-1);
         context.write(key, new Text(ans.toString()));
         System.out.printf("%s:%s\n",key,ans.toString());

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
  public static boolean isNum(String word){
    int n = word.length();
    for(int i=0;i<n;i++)
      if(word.charAt(i)<'0' || word.charAt(i)>'9')
        return false;
    return true;
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

