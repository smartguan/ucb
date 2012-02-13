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
    public static enum ValueUse {NEW_PAIRS};
    


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
                wordK.set(key.toString());
		wordV.set(value.toString());
                context.write(wordK, wordV);

            if (Math.random() < 1.0/(15*denom)) {

                wordV.set(";"+key.toString()+"-1:0;");
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

	Text wordK = new Text();
	Text wordV = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	    	throws IOException, InterruptedException{
	    String firstNodeInfo = new String();
//	    ArrayList<String> edgeList = new ArrayList<String>();
	    String desList = new String(",");
//	    String outKeyList = new String();
	    String valueStr = new String();


	    //Make firstNodeInfo and desList
	    for(Text value:values) {
		valueStr = value.toString();
		if(valueStr.contains("-")) {
		    if(firstNodeInfo.isEmpty()) {
			firstNodeInfo = new String(valueStr);
		    }
		}
		else {
		    if(!desList.contains(","+valueStr+",")) {
			desList = desList + valueStr + ",";
		    }
		}
	    }

	    //desList = des1,des2,des3,|
	    desList = desList.substring(1) + "|";
	    //If they don't contain firstNode, just emit 
	    //(src des1,des2,des3,|)
	    //else (src des1,des2,|;node-1:0;)
	    wordV.set(desList+firstNodeInfo);
	    context.write(key, wordV);




	}

    }



    //Make sure that it only deals with 1 starting Point at 1 loop
    public static class BFSMap extends Mapper<Text, Text, Text, Text>{
        
	public Text wordK = new Text();
	public Text wordV = new Text();

	//Put all 1-->2(from pending to visited)
	//And emits k-v's with k=endpoint, v=NULL|firstNodeList
	//where firtNodeList is the same as the second half of input 'value'
	@Override
	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {

	    String valueStr = value.toString();

	    if(!valueStr.contains("-1")) 
		context.write(key, value);
	    else {
	    String firstNodeList = valueStr.substring(1+valueStr.indexOf("|"));
	    String dList = valueStr.substring(0, valueStr.indexOf("|"));
	    String visitedNode = new String();

	    
//	    int indexM = firstNodeList.lastIndexOf("-1:");
	    int indexL = 0, indexR = 0;
	    int index = firstNodeList.lastIndexOf(";",
				firstNodeList.indexOf("-1:"));

	    String blackNodeList = firstNodeList.substring(0, index);
	    String greyNodeList = firstNodeList.substring(index);
	    int indexM = greyNodeList.indexOf("-1:");

	    //Get the visitedNode and mark it 2
	    //Then emit (endpoint, NULL|firstNodeList)
//	    if(indexM != -1) {
		String step = new String();
		
		//Emit k-v's that are possiblly the next step
		while(!dList.isEmpty()) {
		    wordK.set(dList.substring(0, dList.indexOf(",")));

		    //Increase the accociate steps by 1
		    while(indexM!=-1) {
			indexR = greyNodeList.indexOf(";", indexM+3);
			//Emit k-v (steps, 1)
			context.write(new Text("c"+greyNodeList.substring(
					indexM+3,indexR)), new Text("1"));
			//Increase the steps by 1
			step = String.valueOf(1+Integer.parseInt(
				greyNodeList.substring(indexM+3, indexR)));
			greyNodeList = greyNodeList.substring(0,indexM+3)
					+step+greyNodeList.substring(indexR);
			indexM = greyNodeList.indexOf("-1:", indexM+3);
		    }

        	    
		    
		    wordV.set("NULL|"+greyNodeList);
		    context.write(wordK, wordV);
		    

		    dList = dList.substring(1+dList.indexOf(","));
		}

		//Modify the firstNodeList and set all -1 to -2
		    greyNodeList = greyNodeList.replace("-1:", "-2:");
		//Renew the firstNodeList, and emit k-v
		    wordV.set(valueStr.substring(0, 1+valueStr.indexOf("|"))
		   		    +blackNodeList+greyNodeList);
		    context.write(key, wordV);
//	    }
	    //If there is not -1 in FN's ,just emit the input
//	    else context.write(key, value);
	  }
	}

    }



    //Main BFS code
    public static class BFSReduce extends Reducer<Text, Text, Text, Text> {

	public Text wordK = new Text();
	public Text wordV = new Text();



	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
	    String valueStr = new String();
	    ArrayList<String> toDoList = new ArrayList<String>();
	    String firstNodeList = new String();
	    String desList = new String();
	    String tempToDo = new String();

	    //Used in sorting
	    String tempFirst = new String();
	    int i = 0;
	    


	    long total = 0l;

	    //Count the number of the current emitted steps
	    if(key.toString().charAt(0) == 'c') {
		for(Text value:values) {
		    total = total+1l;
		}
		wordK.set(key.toString().substring(1));
		wordV.set(String.valueOf(total));
		context.write(wordK, wordV);
		context.getCounter(ValueUse.NEW_PAIRS).increment(1l);
		
	    }
	    else {
		//Not Max_Leterations
//		if(key.toString().substring(1) != "19") {
		    for(Text value:values) {
			//If it's a edge
			if(value.toString().contains("|")) {
			    valueStr = value.toString();
			    tempToDo = valueStr.substring(
						1+valueStr.indexOf("|"));
			    //If it's a control in the edge
			    if(valueStr.contains("NULL")) {
				while(!tempToDo.equals(";")) {
				    i = tempToDo.indexOf(";",1);
				    tempFirst = tempToDo.substring(0, i+1);
				    //Check for duplicated item
				    if(!toDoList.contains(tempFirst)) {
				        //Add to the toDoList
				        toDoList.add(new String(tempFirst));
				    }
				    tempToDo = tempToDo.substring(i);
				}
			    }
			    //A edge
			    else {
				if(tempToDo.isEmpty())
				    firstNodeList = ";";
				else firstNodeList = new String(tempToDo);
				desList = valueStr.substring(
					    0, 1+valueStr.indexOf("|"));
			    }
			}
			else context.write(key, value);
		    }

		    //Renew the target edge according to the toDoList
		    //Set -0 to -1 if applicable
		    if(!toDoList.isEmpty()) {
		      int index = 0;
		      String tempNodeT = new String();
		      String tempNodeF = new String();
		      while(!toDoList.isEmpty()) {
			tempToDo = new String(toDoList.get(0));
			index = tempToDo.indexOf("-1");

			//Renew all possible markers in firstNodeList
/*			while(index != -1) {
			    tempNodeT = tempToDo.substring(
				tempToDo.lastIndexOf(";",index),
    				1+tempToDo.indexOf(":",index));
			    tempNodeF = tempNodeT.replace("-1:","-2:");
*/
			    if(!firstNodeList.contains(tempToDo.substring(
						0, index+1))){
				firstNodeList = firstNodeList +
					tempToDo.substring(1);
			    }
//			    index = tempToDo.indexOf("-1:", index+3);
//			}
			toDoList.remove(0);
		      }
		      wordV.set(desList+firstNodeList);
		      context.write(key, wordV);
		    }
		    else if (valueStr.contains("|")) {
			context.write(key, new Text(valueStr));
		    }
/*		}

		//When it reaches the Max_Literations
		//only emit hitogram results
		else {
		    for(Text value:values) {
		        if(!value.toString().contains("|")) {
			    context.write(key, value);
			}
		    }
		}
*/
	    }


	}


    }


    //Histogram Mapper
    public static class HistogramMap extends Mapper<Text, Text, Text, Text>{

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
	    if(!value.toString().contains("|") && !value.toString().isEmpty()){
	        context.write(key, value);
	    }
    	}
    }


   
   public static class HistogramReduce extends Reducer<Text, Text, Text, Text> {

       public Text wordK = new Text();
       public Text wordV = new Text();

       @Override
       public void reduce(Text key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException {

	    for(Text value:values) {
		context.write(key, value);
	    }
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
/*
        // Example of reading a counter
        System.out.println("Read in " + 
                   job.getCounters().findCounter(ValueUse.STEPS).getValue() + 
                           " steps");
*/
        // Repeats your BFS mapreduce
        int i=0, done = 0;
	long curNP = 0; //Stor the counter. 
        // Will need to change terminating conditions to respond to data
        while (i<MAX_ITERATIONS) {
//	    pastNP = job.getCounters().findCounter(ValueUse.NEW_PAIRS).getValue();

            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(BFSMap.class);
            job.setReducerClass(BFSReduce.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
  //          if(i == MAX_ITERATIONS-1 || done == 1) 
//		job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("temp/bfs-" + i + "-out"));
//	    if(i == MAX_ITERATIONS-1 || done == 1) 
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path("temp/bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
	    if(done == 1) break;

	    //Determine whether the BFS has finished
	    curNP = job.getCounters().findCounter(ValueUse.NEW_PAIRS).getValue();
            if(curNP == 0) done = 1;

	    i++;
	    
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
