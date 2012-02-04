/*
  CS 61C Project1: Small World

  Name:Zeyuan_Seth Guan
  Login:cs61c-en

  Name:
  Login:
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    //Number of Starting Points
    public static int numOfStartingPoints = 0;

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};

    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long value;

        public EValue(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public EValue() {
            this(ValueUse.EDGE, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(value);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            value = in.readLong();
        }

        public void set(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public String toString() {
            return use.name() + ": " + value;
        }
    }


    /* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, Text, Text> {
        public long denom;
        private Text wordK = new Text();
        private Text wordV = new Text();
        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
        @Override
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
                                        new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }

        /* Will need to modify to not loose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            // Send edge forward only if part of random subset
                wordK.set("InProgress");
                wordV.set(key.toString().concat("-"+value.toString()));
                context.write(wordK, wordV);

            if (Math.random() < 1.0/denom) {

                wordK.set("Pending");
                wordV.set("*"+key.toString()+"-");
                context.write(wordK, wordV);

	    }
            // Example of using a counter (counter tagged by EDGE)
         //   context.getCounter(ValueUse.EDGE).increment(1);
        }
    }


    /* Insert your mapreduces here
       (still feel free to edit elsewhere) */
//Reducer for load_graph
    public static class LoaderReduce extends Reducer<Text, Text, Text, Text>{
	//Just output all pairs without repeating
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	    	throws IOException, InterruptedException{
	    String sPointList = new String();
	    String sPoint = new String();
	    for(Text value:values)
		{
		    if(key.toString().equals("InProgress")) {
			context.write(key, value);
		    }
		    else {
		        sPoint=value.toString().substring(1,
                                      1+value.toString().indexOf('-'));
	                if(!sPointList.contains(sPoint)) {
			    sPointList=sPointList.concat(sPoint);
			    numOfStartingPoints = numOfStartingPoints + 1;
		            context.write(key, value);
		        }
		    }
  		}
	}

    }

    //Make sure that it only deals with 1 starting Point at 1 loop
    public static class BFSMap extends Mapper<Text, Text, Text, Text>{
        //Store whether the starting point has been picked
        public  String startPoint = "N";
	public Text wordK = new Text();
	//Pick up one starting Point for BFS
	//State is changed to "InProgress"
	@Override
	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
	    String sPoint=value.toString().substring(1,
				1+value.toString().indexOf('-'));
	    if(key.toString().equals("Pending") && startPoint.equals("N")) {
		startPoint = sPoint;
		wordK.set("InProgress");
		context.write(wordK, value);
	    }
	    else context.write(key, value);

	}

    }



    //Main BFS code
    public static class BFSReduce extends Reducer<Text, Text, Text, Text> {

	Text wordK = new Text("Done");
	Text wordV = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {

	    String edgeList = new String("|"); //|1-2|2-3|...
	    String pathQueue = new String();   // p1|p2|...
	    String pastPoints = new String("|");  // |23|123|14|...
	    String path = new String();        // *1-2-3-
	    String endOfPath = new String();   // 12-
	    String newPoint = new String();    //12

	    for(Text value:values) {
		//Pick up SP and all edges and prepare start BFS
		if(key.toString().equals("InProgress")) {
		    if(value.toString().charAt(0) == '*') {
			pathQueue=value.toString()+"|";
			pastPoints=pastPoints.concat(value.toString().substring(
				1, value.toString().length()-1)+"|");
			context.write(wordK, value);
		    }
		    else {
			edgeList=edgeList.concat(value.toString()+"|");
			context.write(key, value);
		    }
		}
		else context.write(key, value);
	    }

	    //Start BFS
	    while(!pathQueue.isEmpty())
	    {
		int index = 0;
		path = pathQueue.substring(0, pathQueue.indexOf("|"));
		pathQueue = pathQueue.substring(1+pathQueue.indexOf("|"));

		if(path.indexOf('-')==path.lastIndexOf('-')) {
		    endOfPath = path.substring(1);
		}
		else 
		    endOfPath = path.substring(path.lastIndexOf('-',
		    	              path.length()-2)+1, path.length()-1)+"-";

		while(edgeList.indexOf("|"+endOfPath, index)!=-1) {
		  //  context.write(wordK, new Text(path));
		    index = edgeList.indexOf("|"+endOfPath, index);
		    newPoint = edgeList.substring(edgeList.indexOf("-",index)+1,
				edgeList.indexOf("|",index+1));
		    index = index + endOfPath.length() + newPoint.length() +1;
		    if(!pastPoints.contains("|"+newPoint+"|")) {
			pastPoints=pastPoints.concat(newPoint+"|");
			pathQueue=pathQueue.concat(path+newPoint+"-|");
			wordV.set(path+newPoint+"-");
			context.write(wordK, wordV);
		    }
		}
	    }
	}


    }


    //MR for histogram
    public static class HistogramMap extends Mapper<Text, Text, LongWritable, LongWritable> {

	@Override
	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
	    
	    int index = 0;
	    long lenLong = 0l;
	    if(key.toString().equals("Done")) {
		while(value.toString().indexOf("-", index)!=-1) {
		    lenLong = lenLong+1l;
		    index = value.toString().indexOf("-", index)+1;
		}
		lenLong = lenLong-1l;
	        context.write(new LongWritable(lenLong), new LongWritable(1l));
	    }

	}
    }


    public static class HistogramReduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {

	    long sum=0l;
	    for(LongWritable value:values) {
		sum = value.get()+sum;
	    }
	    context.write(key, new LongWritable(sum));
	}
    }




    // Shares denom argument across the cluster via DistributedCache
    public static void shareDenom(String denomStr, Configuration conf) {
        try {
	    Path localDenomPath = new Path(DENOM_PATH + "-source");
	    Path remoteDenomPath = new Path(DENOM_PATH);
	    BufferedWriter writer = new BufferedWriter(
				    new FileWriter(localDenomPath.toString()));
	    writer.write(denomStr);
	    writer.newLine();
	    writer.close();
	    FileSystem fs = FileSystem.get(conf);
	    fs.copyFromLocalFile(true,true,localDenomPath,remoteDenomPath);
	    DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
        } catch (IOException ioe) {
            System.err.println("IOException writing to distributed cache");
            System.err.println(ioe.toString());
        }
    }


    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Set denom from command line arguments
        shareDenom(args[2], conf);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("temp/bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Example of reading a counter
        System.out.println("Read in " + 
                   job.getCounters().findCounter(ValueUse.EDGE).getValue() + 
                           " edges");

        // Repeats your BFS mapreduce
        int i=0;
        //Get out of the loop once processed all starting points
	//And No more than 20 literations
        while (i<MAX_ITERATIONS && i<numOfStartingPoints) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(BFSMap.class);
            job.setReducerClass(BFSReduce.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("temp/bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("temp/bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(HistogramMap.class);
//        job.setCombinerClass(Reducer.class);
        job.setReducerClass(HistogramReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("temp/bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
