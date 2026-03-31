import java.io.IOException;
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

public class Bai1 {

    private static final String DELIMITER = ",";

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(DELIMITER);
            if (parts.length >= 2) {
                outKey.set(parts[0].trim());
                outValue.set("M_" + parts[1].trim());
                context.write(outKey, outValue);
            }
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(DELIMITER);
            if (parts.length >= 3) {
                outKey.set(parts[1].trim());
                outValue.set("R_" + parts[2].trim());
                context.write(outKey, outValue);
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, Text, Text, NullWritable> {
        private String maxMovie = "";
        private double maxRating = 0.0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown Movie";
            double sumRating = 0.0;
            int count = 0;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M_")) {
                    movieTitle = v.substring(2);
                } else if (v.startsWith("R_")) {
                    try {
                        sumRating += Double.parseDouble(v.substring(2));
                        count++;
                    } catch (NumberFormatException e) {

                    }
                }
            }

            if (count > 0) {
                double avgRating = sumRating / count;

                String resultStr = String.format(Locale.US, "%s AverageRating: %.2f (TotalRatings: %d)", movieTitle,
                        avgRating, count);
                context.write(new Text(resultStr), NullWritable.get());

                if (count >= 5) {
                    if (avgRating > maxRating) {
                        maxRating = avgRating;
                        maxMovie = movieTitle;
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String finalResult = String.format(Locale.US,
                        "%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.",
                        maxMovie, maxRating);
                context.write(new Text(finalResult), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage: hadoop jar <jar_file> Bai1 <path_to_movies.txt> <path_to_ratings_dir> <output_dir>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analyzer");
        job.setJarByClass(Bai1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setReducerClass(MovieReducer.class);

        job.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}