import java.io.IOException;
import java.util.regex.*;
import java.util.Set;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;

import com.google.gson.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmazonProductMapReduce extends Configured implements Tool {

  protected static final Logger LOG = LoggerFactory.getLogger(AmazonProductMapReduce.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(HBaseConfiguration.create(), new AmazonProductMapReduce(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Need 1 argument (hdfs output path), got: " + args.length);
      return -1;
    }
     
    Job job = Job.getInstance(getConf(), "AmazonProductMapReduce");
    job.setJarByClass(AmazonProductMapReduce.class);
    
    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don't set to true for MR jobs

    TableMapReduceUtil.initTableMapperJob(
      "rfox12:products",        // input HBase table name
      scan,             // Scan instance to control CF and attribute selection
      MapReduceMapper.class,   // mapper
      Text.class,             // mapper output key
      IntWritable.class,             // mapper output value
      job);

    // For counter-only output    
    // job.setOutputFormatClass(NullOutputFormat.class);
    
    // For file output (text -> number)
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    job.setReducerClass(MapReduceReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    TableMapReduceUtil.addDependencyJars(job);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class MapReduceMapper extends TableMapper<Text, IntWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(MapReduceMapper.class);
    
    private static final byte[] CF_NAME = Bytes.toBytes("cf");
    private static final byte[] QUALIFIER = Bytes.toBytes("product_data");
    private final static IntWritable one = new IntWritable(1);
    
    private Counter productsProcessed;
    private JsonParser parser;

    @Override
    protected void setup(Context context) {
    	parser = new JsonParser();
	productsProcessed = context.getCounter("AmazonProductMapReduce", "Products Processed");
    }
  
    @Override
    public void map(ImmutableBytesWritable rowKey, Result value, Context context) throws InterruptedException, IOException {
	try {
		String jsonString = new String(value.getValue(CF_NAME, QUALIFIER));
		JsonElement jsonTree = parser.parse(jsonString);
		for (Map.Entry<String, JsonElement> entry : jsonTree.getAsJsonObject().entrySet()) {
			context.write(new Text(entry.getKey()),one);
			JsonElement jv = entry.getValue();
			
			if (jv.isJsonNull()) {
				context.write(new Text(entry.getKey()+"-null"),one);
				
			} else if (jv.isJsonPrimitive()) {
				if (jv.getAsJsonPrimitive().isBoolean()){
					context.write(new Text(entry.getKey()+"-boolean"),one);
				}else if (jv.getAsJsonPrimitive().isNumber()){
					context.write(new Text(entry.getKey()+"-number"),one);
				}else if (jv.getAsJsonPrimitive().isString()){
					if(jv.getAsString().trim().isEmpty()){
						context.write(new Text(entry.getKey()+"-blank-string"),one);
					}else{
						context.write(new Text(entry.getKey()+"-string"),one);
					}
				}else{
					context.write(new Text(entry.getKey()+"-primitive-unknown"),one);
				}
				
			} else if (jv.isJsonArray()) {
				if(jv.getAsJsonArray().size() == 0){
					context.write(new Text(entry.getKey()+"-empty-array"),one);
				}else{
					context.write(new Text(entry.getKey()+"-array"),one);
				}
				
			} else if (jv.isJsonObject()) {
				Set<Map.Entry<String, JsonElement>> innerEntrySet = jv.getAsJsonObject().entrySet();
				if(innerEntrySet.isEmpty()){
					context.write(new Text(entry.getKey()+"-empty-object"),one);
				}else{
					context.write(new Text(entry.getKey()+"-object"),one);
				}
				
			} else {
				context.write(new Text(entry.getKey()+"-unknown"),one);
			}
		}
		
		productsProcessed.increment(1);
        } catch (Exception e) {
          LOG.error("Error in MAP process: " + e.getMessage(), e);
        }
    }
  }
  
  // Reducer to simply sum up the values with the same key (text)
  public static class MapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}

