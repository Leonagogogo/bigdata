package part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class MovieRatings {

    private static HashMap<Text, IntWritable> averageMap = new HashMap<Text, IntWritable>();

    public static Number parseWord(String temp) throws ParseException {

        Number result;
        DecimalFormat df = new DecimalFormat("###.###");
        df.setMaximumFractionDigits(3);
        result = df.parse(temp);

        return result;
    }

    public static class movieRatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieId = new Text();
        private Text movieRating = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String cleanLine = value.toString();
            StringTokenizer itr = new StringTokenizer(cleanLine);
            while(itr.hasMoreTokens()){
                String[] record = itr.nextToken().trim().split(",");
                if(record[0].trim().equals("userId"));
                else{
                    String Id = record[1].trim();
                    String rating = record[2].trim();
                    String result = rating + ", 1";

                    movieId.set(Id);
                    movieRating.set(result);
                    context.write(movieId, movieRating);
                }
            }
        }
    }

    public static class movieRatingReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private Map<Text, DoubleWritable> countMap = new HashMap<Text, DoubleWritable>();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double tempSum = 0;
            int tempCount = 0;
            for (Text val : values) {
                String[] s = val.toString().split(",");
                try{
                    tempSum += parseWord(s[0].trim()).doubleValue();
                    tempCount += parseWord(s[1].trim()).intValue();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

            double average = tempSum /  tempCount;
            countMap.put(new Text(key), new DoubleWritable(average));
        }

     //   @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, DoubleWritable> sortedMap = MiscUtils.sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 20) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    public static class movieRatingSumCombiner extends Reducer<Text,Text,Text,Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result =null;
            float tempSum = 0;
            int tempCount = 0;
            for (Text val : values) {
                String[] s = val.toString().split(",");
                try{
                    tempSum += parseWord(s[0].trim()).floatValue();
                    tempCount += parseWord(s[1].trim()).intValue();
                    result = String.valueOf(tempSum) + "," + String.valueOf(tempCount);
                }catch(Exception e){
                    e.printStackTrace();
                 }
            }
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
        conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "moving rating");

        String uri = "hdfs://cshadoop1/movielens/ratings.csv";

        job.setJarByClass(MovieRatings.class);
        job.setMapperClass(movieRatingMapper.class);
        job.setCombinerClass(movieRatingSumCombiner.class);
        job.setReducerClass(movieRatingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(uri));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



