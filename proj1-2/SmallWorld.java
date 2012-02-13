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
                wordK.set("1");
                wordV.set(key.toString().concat("-"+value.toString()));
                context.write(wordK, wordV);

            if (Math.random() < 1.0/denom) {

                wordK.set("1");
                wordV.set("*"+key.toString()+":0");
                context.write(wordK, wordV);

	    }
            // Example of using a counter (counter tagged by EDGE)
         //   context.getCounter(ValueUse.EDGE).increment(1);
        }
    }


    // Insert your mapreduces here
    //   (still feel free to edit elsewhere) 
//Reducer for load_graph
    public static class LoaderReduce extends Reducer<Text, Text, Text, Text>{
	//Just output all pairs without repeating


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	    	throws IOException, InterruptedException{
	    String edgeList = new String("|"); //e.g. |12-345|1-32|32-10|
	    String sNodeList = new String();  //(*)1:0|*23:0| ...
	    String valueStr = new String();

	    Text wordK = new Text();
	    Text wordV = new Text();

	    for(Text value:values){
		valueStr = value.toString();
		if(valueStr.charAt(0)=='*') {
		   if(!sNodeList.contains(valueStr+"|")) {
		        sNodeList = sNodeList.concat(valueStr+"|");
		    }
		}
		else {
		    if(!edgeList.contains("|"+valueStr+"|")) {
			edgeList = edgeList.concat(valueStr+"|");
		    }
		}
  	    }


	    //Construct the emmitting k-v pairs
	    while(!sNodeList.isEmpty()) {
		String node = sNodeList.substring(0, sNodeList.indexOf("|"));
		String outEdgeList = new String(edgeList);
		String searchNode = node.substring(1,node.indexOf(":"));
		wordK.set(node);
		
//		context.write(new Text(sNodeList), new Text(edgeList));
		//Delete all edges ending at node
		while(outEdgeList.contains("-"+searchNode+"|")) {
		    int indexM = outEdgeList.indexOf("-"+searchNode+"|"); //-
		    int indexL = outEdgeList.lastIndexOf("|", indexM); //left |
		    int indexR = outEdgeList.indexOf("|", indexM); //right |

		    outEdgeList = outEdgeList.substring(0, indexL).concat(
					    outEdgeList.substring(indexR));
		}
		wordV.set(searchNode.concat("."+outEdgeList));
		context.write(wordK, wordV);
		sNodeList = sNodeList.substring(sNodeList.indexOf("|")+1);
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
	    Text wordK = new Text();
	    Text wordV = new Text();
	    String keyStr = key.toString();

	    context.write(key, value);

	    if(key.toString().charAt(0) == '*') {
		wordK.set("~"+keyStr.substring(keyStr.indexOf(":")+1));
	        wordV.set("1");
	        context.write(wordK, wordV);
	    }

	}

    }



    //Main BFS code
    public static class BFSReduce extends Reducer<Text, Text, Text, Text> {


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {

	    int count=0;
	    Text wordK = new Text();
	    Text wordV = new Text();
	    String node = new String();
	    String newNodeList = new String();
	    String edgeList = new String();
	    

	    if(!key.toString().contains("*")) {
		if(!key.toString().contains("~")) {
		    for(Text value:values) {
		        context.write(key, value);
		    }
		}
		else {
		    for(Text value:values) {
		        count = count+1;
		    }
		    wordK.set(key.toString().substring(1));
		    wordV.set(Integer.toString(count));
		    context.write(wordK, wordV);
		}
	    }

	    else {
	        for(Text value:values) {
	
		    node = value.toString().substring(0,
						value.toString().indexOf("."));
		    if(edgeList.isEmpty()) {
			edgeList = value.toString().substring(
					    value.toString().indexOf(".")+1);
		    }
			

		    int index = edgeList.indexOf("|"+node+"-");
		    String nextPath = new String();
		    String newNode = new String();

	            while(index!=-1) {
		        nextPath = edgeList.substring(index+1,
				     edgeList.indexOf("|",index+1));
			newNode = nextPath.substring(nextPath.indexOf("-")+1);
			newNodeList = newNodeList.concat(newNode+"|");
		 	//Delete all edges ending at node
			while(edgeList.contains("-"+newNode+"|")) {
    			    int indexM = edgeList.indexOf(
						"-"+newNode+"|"); //-
    			    int indexL = edgeList.lastIndexOf(
						"|", indexM); //left |
    			    int indexR = edgeList.indexOf("|", indexM); //right |

    			    edgeList = edgeList.substring(0,
					indexL).concat(edgeList.substring(
							indexR));
			}
		        index = edgeList.indexOf("|"+node+"-");
		    }

	        }
		
		String temp = key.toString();
		int i = 1 + Integer.parseInt(key.toString().substring(
						key.toString().indexOf(":")+1));
		wordK.set(temp.substring(0, temp.indexOf(":")+1)
				+ Integer.toString(i));

		while(!newNodeList.isEmpty()) {
		    wordV.set(newNodeList.substring(0,
				newNodeList.indexOf("|"))+"."+edgeList);
		 
		    context.write(wordK, wordV);
		    newNodeList = newNodeList.substring(
					newNodeList.indexOf("|")+1);
		}

	    }


	}


    }

/*
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


*/

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
        // Will need to change terminating conditions to respond to data
        while (i<20) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(BFSMap.class);
            job.setReducerClass(BFSReduce.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            if(i == 19) job.setOutputFormatClass(TextOutputFormat.class);
	    else job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("temp/bfs-" + i + "-out"));
	    if(i == 19) FileOutputFormat.setOutputPath(job, new Path(args[1]));
            else FileOutputFormat.setOutputPath(job, new Path("temp/bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }
/*
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
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
*/
    }
}
