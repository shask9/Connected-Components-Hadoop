package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable {
	
	private Integer tag;
	private Integer group;
	private Integer vertexId;
	private ArrayList<Integer> adjacentVectors;

	public Integer getTag() {
		return tag;
	}
	
	public Integer getGroup() {
		return group;
	}
	
	public Integer getVertexId() {
		return vertexId;
	}
	
	public ArrayList<Integer> getAdjacentVectors() {
		return adjacentVectors;
	}
	
	public Vertex() {
		this.tag = 0;
		this.group = 0;
		this.vertexId = 0;
		this.adjacentVectors = new ArrayList<Integer>();
	}
	
	public Vertex(Integer tag, Integer group, Integer vertexId, ArrayList<Integer> adjacentVecors) {
		this.tag = tag;
		this.group = group;
		this.vertexId = vertexId;
		this.adjacentVectors = adjacentVecors;
	}
	
	public Vertex(Integer tag, Integer group) {
		this.tag = tag;
		this.group = group;
		this.vertexId = 0;
		this.adjacentVectors = new ArrayList<Integer>();
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		tag = input.readInt();
		group = input.readInt();
		vertexId = input.readInt();
		
		adjacentVectors.clear();
		IntWritable size = new IntWritable();
		size.readFields(input);
		for(int i=0;i<size.get();i++) {
			IntWritable adjacentVector = new IntWritable();
			adjacentVector.readFields(input);
			adjacentVectors.add(adjacentVector.get());
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(tag);
		output.writeInt(group);
		output.writeInt(vertexId);
	
		IntWritable size = new IntWritable(adjacentVectors.size());
		size.write(output);
		for(Integer adjacentVector : adjacentVectors) {
			output.writeInt(adjacentVector);
		}
	}
	
	/*public String toString() {
		return tag + " " + group + " " + vertexId + " " + adjacentVectors + " " ;
	}*/
	
}

public class Graph {
	
	public static class MapVertexId extends Mapper<Object, Text, IntWritable, Vertex> {
		
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String readInput = value.toString();
			String[] tokenizeInput = readInput.split(",");
			
			Integer vertexId = new Integer (Integer.parseInt(tokenizeInput[0]));
			ArrayList<Integer> adjacentVectors = new ArrayList<Integer>();
			for(int i=1;i<tokenizeInput.length;i++) {
				adjacentVectors.add(Integer.parseInt(tokenizeInput[i]));
			}
			
			Vertex v = new Vertex(0, vertexId, vertexId, adjacentVectors);
			
			context.write(new IntWritable(vertexId), v);
			//System.out.print(vertexId + " : " + v );
		}
	}
	

	public static class MapVertexGroup extends Mapper<IntWritable, Vertex, IntWritable, Vertex> {
		
		@Override
		public void map(IntWritable key, Vertex value, Context context) 
				throws IOException, InterruptedException {
			
			context.write(new IntWritable(value.getVertexId()), value);
			
			for(Integer adjacent : value.getAdjacentVectors()) {
				context.write(new IntWritable(adjacent),new Vertex(1,value.getGroup()));
			}
		}
	}
	
	public static class ReduceVertexGroup extends Reducer<IntWritable, Vertex, IntWritable, Vertex> {
		
		@Override
		public void reduce(IntWritable key, Iterable<Vertex> values, Context context) 
				throws IOException, InterruptedException {
			
			Integer assignGroup = Integer.MAX_VALUE;
			ArrayList<Integer> adjacentVectors = new ArrayList<Integer>();
			for(Vertex v : values) {
				Integer tag = (Integer) v.getTag();
				
				if(tag.equals(0)) {
					adjacentVectors = new ArrayList<Integer>(v.getAdjacentVectors());
				}
				
				assignGroup = min(assignGroup,v.getGroup());
				
				
			}
			
			Vertex assignVertex = new Vertex(0, assignGroup, key.get(), adjacentVectors);
			context.write(new IntWritable(assignGroup), assignVertex);
		}
		
		public Integer min(Integer i,Integer j) {
			if(i<j) {
				return i;
			} else {
				return j;
			}
		}
	}
	
	public static class MapGroupCount extends Mapper<IntWritable, Vertex, IntWritable, IntWritable> {
		
		@Override
		public void map(IntWritable key, Vertex value, Context context) 
				throws IOException, InterruptedException {
			
			context.write(key, new IntWritable(1));
		}
	}
	
	public static class ReduceGroupCount extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			Integer connectedComponents = 0;
			
			for (IntWritable value : values) {
				connectedComponents += value.get();
			}
			
			context.write(key, new IntWritable(connectedComponents));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job1 = Job.getInstance();
		job1.setJobName("mapOutKeys");
		job1.setJarByClass(Graph.class);
		
		job1.setMapperClass(MapVertexId.class);
		job1.setNumReduceTasks(0);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Vertex.class);
		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Vertex.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
		
		job1.waitForCompletion(true);
		
		for(int i=0;i<5;i++) {
			Job job2 = Job.getInstance();
			job2.setJobName("mapVertexGroup : Pass " + i);
			job2.setJarByClass(Graph.class);
			
			job2.setMapperClass(MapVertexGroup.class);
			job2.setReducerClass(ReduceVertexGroup.class);
			
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Vertex.class);
			
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Vertex.class);
			
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i+1)));
			
			job2.waitForCompletion(true);
		}
		
		Job job3 = Job.getInstance();
		job3.setJobName("mapGroupCount");
		job3.setJarByClass(Graph.class);
		
		job3.setMapperClass(MapGroupCount.class);
		job3.setReducerClass(ReduceGroupCount.class);
		
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(IntWritable.class);
		
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(IntWritable.class);
	
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		
		job3.waitForCompletion(true);
	}
}

