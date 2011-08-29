
package org.viirya.graph;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
/*
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
*/
import org.apache.hadoop.fs.*;
/*
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
*/
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;


public class GraphEvaluationMapReduce {

    private static boolean compression = false;

    private static int number_of_top_k_edges = 1000;


    public static StringTokenizer tokenize(String line, String pattern) {
        StringTokenizer tokenizer = new StringTokenizer(line, pattern);
        return tokenizer;
    }

    public static StringTokenizer tokenize(Text value, String pattern) {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, pattern);
        return tokenizer;
    }

    
    public static class LoadGroundTruthAndGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {
 
            StringTokenizer tokenizer = tokenize(value, " (),\t");

            String image_id = null;
            String output_value = null;

            if (tokenizer.countTokens() == 1) {
                /* load ground truth */

                String input_filename = ((FileSplit) context.getInputSplit()).getPath().getName();
                tokenizer = tokenize(input_filename, ".");
                input_filename = tokenizer.nextToken();

                image_id = value.toString();
                output_value = "g:" + input_filename;

                context.write(new Text(image_id), new Text(output_value));

            } else if (tokenizer.countTokens() > 1) {
                /* load graph data */

                image_id = tokenizer.nextToken();
                String img_k = tokenizer.nextToken();
                float sim = Float.parseFloat(tokenizer.nextToken());
                
                output_value = img_k + " " + Float.toString(sim);
                context.write(new Text(image_id), new Text(output_value));

                output_value = image_id  + " " + Float.toString(sim);
                context.write(new Text(img_k), new Text(output_value));
            }

            //if (image_id != null && output_value != null)
            //    context.write(new Text(image_id), new Text(output_value));

        }

    }
 
    public static class LoadGroundTruthAndGraphReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Float> edges = new HashMap<String, Float>();
            String ground_truth = null;

            for (Text value: values) {

                StringTokenizer tokenizer = tokenize(value, ":");

                if (tokenizer.countTokens() == 2) {
                    // ground truth

                    tokenizer.nextToken();

                    // an image could occur in more than 1 ground truth file
                    if (ground_truth == null)
                        ground_truth = tokenizer.nextToken(); 
                    else
                        ground_truth = ground_truth + "%" + tokenizer.nextToken();


                } else if (tokenizer.countTokens() == 1) {
                    // graph data

                    tokenizer = tokenize(value, " ");

                    String img_k = tokenizer.nextToken();
                    float similarity = Float.parseFloat(tokenizer.nextToken());

                    edges.put(img_k, new Float(similarity));

                }

            }

            
            // if the key is not in ground truth, skip it
            if (ground_truth == null)
                return;
 
            // when the image has no links, add a fake one for MAP computation
            if (edges.size() == 0) {
                edges.put("-1", new Float(0.1f));
            }
 
            // going to sort the edges

            List<Map.Entry<String, Float>> list = new Vector<Map.Entry<String, Float>>(edges.entrySet());
            java.util.Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
                public int compare(Map.Entry<String, Float> entry, Map.Entry<String, Float> entry1) {
                    // Return 0 for a match, -1 for less than and +1 for more then
                    return (entry.getValue().equals(entry1.getValue()) ? 0 : (entry.getValue() > entry1.getValue() ? -1 : 1));
                }
            });

            edges.clear();
 
            StringBuffer strbuf = new StringBuffer();
            int edge_counter = 0;
            Iterator itr = list.iterator();
            while(itr.hasNext() && edge_counter < number_of_top_k_edges) {
                edge_counter++;

                Map.Entry<String, Float> entry = (Map.Entry<String, Float>)itr.next();
                String img_k = entry.getKey();
                //Float sim = entry.getValue();
                strbuf.append("(" + img_k + " " + Integer.toString(edge_counter) + ")");
            }

            context.write(key, new Text(ground_truth + ":" + strbuf.toString()));
            

        }
    }
 
 
    public static class CountTopKMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {
        
            StringTokenizer tokenizer = tokenize(value, "\t");

            String image_id = null;
            String output_value = null;

            if (tokenizer.countTokens() == 1) {
                // load ground truth

                String input_filename = ((FileSplit) context.getInputSplit()).getPath().getName();
                tokenizer = tokenize(input_filename, ".");
                input_filename = tokenizer.nextToken();

                image_id = value.toString();
                output_value = "g:" + input_filename;

                context.write(new Text(image_id), new Text(output_value));
 

            } else if (tokenizer.countTokens() > 1) {
                // load top k information
                
                tokenizer = tokenize(value, "\t:");

                image_id = tokenizer.nextToken();
                String source_ground_truth = tokenizer.nextToken();

                tokenizer = tokenize(tokenizer.nextToken(), " ()");
 
                while (tokenizer.hasMoreTokens()) {
                    String image_target = tokenizer.nextToken();
                    String rank = tokenizer.nextToken();
 
                    output_value = "(" + source_ground_truth + "#" + image_id + " " + rank + ")";
                    context.write(new Text(image_target), new Text(output_value));
                }
            }
 
        }
    }

 
    public static class CountTopKReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //String ground_truth = null;
            HashMap<String, Float> edges = new HashMap<String, Float>();
            HashMap<String, Boolean> ground_truth = new HashMap<String, Boolean>();
 
            for (Text value: values) {

                StringTokenizer tokenizer = tokenize(value, ":");

                if (tokenizer.countTokens() == 2) {
                    // ground truth

                    tokenizer.nextToken();
                    ground_truth.put(tokenizer.nextToken(), true);
                    /*
                    if (ground_truth == null)
                        ground_truth = tokenizer.nextToken(); 
                    else
                        ground_truth = ground_truth + "%" + tokenizer.nextToken();
                    */
                    

                } else if (tokenizer.countTokens() == 1) {

                    tokenizer = tokenize(value, "#()");

                    String source_ground_truth = tokenizer.nextToken();

                    tokenizer = tokenize(tokenizer.nextToken(), " ");

                    String source_image_id = tokenizer.nextToken();
                    int rank = Integer.parseInt(tokenizer.nextToken());

                    if (edges.containsKey(source_image_id + ":" + source_ground_truth)) {
                        edges.put(source_image_id + ":" + source_ground_truth, edges.get(source_image_id + ":" + source_ground_truth) + (1 / (float) rank));
                    } else {
                        edges.put(source_image_id + ":" + source_ground_truth, (1 / (float) rank));
                    } 
                }

            }


            if (key.toString().equals("-1"))
                ground_truth.put("fake node", true);
                //ground_truth = "fake node";

            for (Map.Entry<String, Float> entry : edges.entrySet()) {
                String[] cur_key = entry.getKey().split(":");
                StringTokenizer source_ground_truth = tokenize(cur_key[1], "%"); 

                if (ground_truth.size() == 0 || ground_truth.containsKey("fake node")) {
                    while (source_ground_truth.hasMoreTokens()) {
                        context.write(new Text(cur_key[0] + ":" + source_ground_truth.nextToken()), new Text(Float.toString(0.0f)));
                    }
                }
                else {
                    while (source_ground_truth.hasMoreTokens()) {
                        String s_gt = source_ground_truth.nextToken();
                        if (ground_truth.containsKey(s_gt))
                            context.write(new Text(cur_key[0] + ":" + s_gt), new Text(edges.get(cur_key[0] + ":" + cur_key[1]).toString()));
                        else
                            context.write(new Text(cur_key[0] + ":" + s_gt), new Text(Float.toString(0.0f)));
                    }
                }
            }            
        }
    }
 
    public static class AveragePrecisionMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {

 
            StringTokenizer tokenizer = tokenize(value, "\t");

            String image_id = tokenizer.nextToken();
            String output_value = tokenizer.nextToken();

            context.write(new Text(image_id), new Text(output_value));
 
        }

    }
 
    public static class AveragePrecisionReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            ArrayList rank_list = new ArrayList();
            float sum = 0.0f;
            int count = 0;
            int non_zero_count = 0;

            for (Text value: values) {
                //sum += Float.parseFloat(value.toString());
                //count++;
                if (Float.parseFloat(value.toString()) > 0.0f) {
                    rank_list.add(new Float(Float.parseFloat(value.toString())));
                    non_zero_count++;
                }
            }

            java.util.Collections.sort(rank_list, new Comparator<Float>() {
                public int compare(Float entry, Float entry1) {
                    // Return 0 for a match, -1 for less than and +1 for more then
                    return (entry.floatValue() == entry1.floatValue()) ? 0 : (entry.floatValue() > entry1.floatValue() ? -1 : 1);
                }
            });

            Iterator itr = rank_list.iterator();
            count = 1;
            sum = 0.0f;
            while(itr.hasNext()) {
                sum += ((float)count++ * (Float)itr.next());
            }

            String[] cur_key = key.toString().split(":");

            if (non_zero_count > 0)
                context.write(new Text(cur_key[1]), new Text(Float.toString(sum / (float)non_zero_count)));
            else
                context.write(new Text(cur_key[1]), new Text(Float.toString(0.0f)));

        }
    }
 
    public static class MeanAveragePrecisionMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws NumberFormatException, IOException, InterruptedException {

 
            StringTokenizer tokenizer = tokenize(value, "\t");
            
            String category = tokenizer.nextToken(); 
            //String image_id = tokenizer.nextToken();
            String output_value = tokenizer.nextToken();

            context.write(new Text(category), new Text(output_value)); 
            //context.write(new Text("MAP"), new Text(output_value));
 
        }

    }
 
    public static class MeanAveragePrecisionReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float sum = 0.0f;
            int count = 0;

            for (Text value: values) {
                sum += Float.parseFloat(value.toString());
                count++;
            }

            context.write(key, new Text(Float.toString(sum / (float)count)));

        }
    }
 
    private static void setJobConfCompressed(Configuration conf) {
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
    }


    public static void main(String[] args) throws Exception {

        String groundtruth_path = null;
        String graph_path = null;

        if (args.length < 3) {
            System.out.println("Usage: GraphEvaluationMapReduce <groundtruth path> <graph path> <top k> [compress]");    
            System.exit(0);
        }

        groundtruth_path = args[0];
        graph_path = args[1];

        number_of_top_k_edges = Integer.parseInt(args[2]);

        if (args.length == 4 && args[3].equals("compress"))
            compression = true;

        loadGroundTruthAndGraph(groundtruth_path, graph_path);
        countTopK(groundtruth_path);
        computeAP();
        computeMAP();

    }
 
    public static void loadGroundTruthAndGraph(String groundtruth_path, String graph_path) throws Exception {


        Configuration conf = new Configuration();
        conf.setLong("dfs.block.size",134217728);
        if (compression)
            setJobConfCompressed(conf);

        Job job_loaddata = new Job(conf);
        job_loaddata.setJarByClass(GraphEvaluationMapReduce.class);
        job_loaddata.setJobName("LoadGroundTruthAndGraph");

        FileInputFormat.setInputPaths(job_loaddata, new Path(groundtruth_path + "/*.txt"), new Path(graph_path + "/*.gz"));
        FileOutputFormat.setOutputPath(job_loaddata, new Path("output/graph_evaluation/step1"));

        job_loaddata.setOutputKeyClass(Text.class);
        job_loaddata.setOutputValueClass(Text.class);
        job_loaddata.setMapOutputKeyClass(Text.class);
        job_loaddata.setMapOutputValueClass(Text.class);
        job_loaddata.setMapperClass(LoadGroundTruthAndGraphMapper.class);
        job_loaddata.setReducerClass(LoadGroundTruthAndGraphReducer.class);
        job_loaddata.setNumReduceTasks(19);

        try {
            job_loaddata.waitForCompletion(true);
        } catch(Exception e){
            e.printStackTrace();
        }

    }
 
    public static void countTopK(String groundtruth_path) throws Exception {


        Configuration conf = new Configuration();
        conf.setLong("dfs.block.size",134217728);
        if (compression)
            setJobConfCompressed(conf);

        Job job = new Job(conf);
        job.setJarByClass(GraphEvaluationMapReduce.class);
        job.setJobName("CountTopK");

        FileInputFormat.setInputPaths(job, new Path(groundtruth_path + "/*.txt"), new Path("output/graph_evaluation/step1"));
        FileOutputFormat.setOutputPath(job, new Path("output/graph_evaluation/step2"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(CountTopKMapper.class);
        job.setReducerClass(CountTopKReducer.class);
        job.setNumReduceTasks(19);

        try {
            job.waitForCompletion(true);
        } catch(Exception e){
            e.printStackTrace();
        }

    }
 
    public static void computeAP() throws Exception {

        Configuration conf = new Configuration();
        conf.setLong("dfs.block.size",134217728);
        if (compression)
            setJobConfCompressed(conf);

        Job job = new Job(conf);
        job.setJarByClass(GraphEvaluationMapReduce.class);
        job.setJobName("ComputeAP");

        FileInputFormat.setInputPaths(job, new Path("output/graph_evaluation/step2"));
        FileOutputFormat.setOutputPath(job, new Path("output/graph_evaluation/step3"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(AveragePrecisionMapper.class);
        job.setReducerClass(AveragePrecisionReducer.class);
        job.setNumReduceTasks(19);

        try {
            job.waitForCompletion(true);
        } catch(Exception e){
            e.printStackTrace();
        }

    } 
 
    public static void computeMAP() throws Exception {

        Configuration conf = new Configuration();
        conf.setLong("dfs.block.size",134217728);
        if (compression)
            setJobConfCompressed(conf);

        Job job = new Job(conf);
        job.setJarByClass(GraphEvaluationMapReduce.class);
        job.setJobName("ComputeAP");

        FileInputFormat.setInputPaths(job, new Path("output/graph_evaluation/step3"));
        FileOutputFormat.setOutputPath(job, new Path("output/graph_evaluation/step4"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MeanAveragePrecisionMapper.class);
        job.setReducerClass(MeanAveragePrecisionReducer.class);
        job.setNumReduceTasks(1);

        try {
            job.waitForCompletion(true);
        } catch(Exception e){
            e.printStackTrace();
        }

    }  
}


