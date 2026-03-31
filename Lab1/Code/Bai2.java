import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    private static final String DELIMITER = ",";

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("MovieID") || line.trim().isEmpty()) return;

            String delim = line.contains("::") ? "::" : ",";
            String[] parts = line.split(delim);
            
            if (parts.length >= 3) {
                String movieID = parts[0].trim();
                String genresStr = parts[parts.length - 1].trim(); 
                
                outKey.set(movieID);
                
                String[] genres = genresStr.split("\\|");
                for (String genre : genres) {
                    outValue.set("M_" + genre.trim());
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("UserID") || line.trim().isEmpty()) return;

            String delim = line.contains("::") ? "::" : ",";
            String[] parts = line.split(delim);
            
            if (parts.length >= 3) {
                outKey.set(parts[1].trim());          
                outValue.set("R_" + parts[2].trim());  
                context.write(outKey, outValue);
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, NullWritable> {
        private Map<String, double[]> genreStats = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movieGenres = new ArrayList<>();
            double sumRating = 0.0;
            int count = 0;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M_")) {
                    movieGenres.add(v.substring(2)); 
                } else if (v.startsWith("R_")) {
                    try {
                        sumRating += Double.parseDouble(v.substring(2)); 
                        count++;
                    } catch (NumberFormatException e) { }
                }
            }

            if (count > 0 && !movieGenres.isEmpty()) {
                for (String genre : movieGenres) {
                    double[] stats = genreStats.getOrDefault(genre, new double[]{0.0, 0.0});
                    stats[0] += sumRating; 
                    stats[1] += count;     
                    genreStats.put(genre, stats);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, double[]> entry : genreStats.entrySet()) {
                String genre = entry.getKey();
                double totalSum = entry.getValue()[0];
                int totalCount = (int) entry.getValue()[1];
                
                if (totalCount > 0) {
                    double avgRating = totalSum / totalCount;
                    String resultStr = String.format(Locale.US, "%s Avg:%.2f, Count: %d", genre, avgRating, totalCount);
                    context.write(new Text(resultStr), NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analyzer");
        job.setJarByClass(Bai2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setReducerClass(GenreReducer.class);
        job.setNumReduceTasks(1); 
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}