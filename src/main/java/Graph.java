import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.lang.Iterable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;


class Vertex implements Writable{
    public int tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    /* ... */
    Vertex(int tag, long group, long VID, Vector<Long> adjacent){
    this.tag = tag;
    this.group = group;
    this.VID = VID;
    this.adjacent = adjacent;
    }
    Vertex()
    {

    }
    public String printadj()
    {
        String s="";
        for(int i=0;i<adjacent.size();i++)
        {
            s=s+adjacent.get(i);
        }
        return s;
    }
    public String toString()
    {
        return this.tag+","+this.group+","+this.VID+","+this.printadj();
    }
    Vertex(int tag, long group){
        this.tag = tag;
        this.group = group;
        this.VID=0;
        this.adjacent=new Vector();
    }
     public void write ( DataOutput out ) throws IOException {
        out.writeShort(this.tag);
        out.writeLong(this.group);
        out.writeLong(this.VID);
        out.writeInt(adjacent.size());
        for(int i=0;i<adjacent.size();i++)
        {
            out.writeLong(adjacent.get(i));
        }
    }
    public void readFields ( DataInput in ) throws IOException {
        tag=in.readShort();
        group=in.readLong();
        VID=in.readLong();
        int size=in.readInt();
        Vector<Long> adjacenttemp=new Vector();
        for(int i=0;i<size;i++)
        {
            long node=in.readLong();
            adjacenttemp.addElement(node);
        }
        adjacent=adjacenttemp;
    }
}

public class Graph {

    /* ... */

    public static class GraphFirstMapper extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long VID = new Long(s.nextInt());
            Vector<Long> adjacent = new Vector<Long>();
            while(s.hasNext()){
                adjacent.add(new Long(s.nextInt()));
            }
            context.write(new LongWritable(VID), new Vertex(0,VID,VID,adjacent));
        }
    }

    public static class GraphSecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex>{
        @Override
        public void map ( LongWritable key, Vertex ver, Context context )
                        throws IOException, InterruptedException {
            context.write(new LongWritable(ver.VID), ver);
            // System.out.println(ver.toString());
            for(int i = 0;i <ver.adjacent.size();i++){
                context.write(new LongWritable(ver.adjacent.get(i)), new Vertex(1,ver.group));
            }
        }
    }

    public static class GraphSecondReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{
 
        public void reduce (LongWritable VID, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
            long m = Long.MAX_VALUE;
            Vector<Long> adj = new Vector();
            for(Vertex v: values){
                if(v.tag == 0){
                    adj = v.adjacent;
                   
                }
                m=Math.min(m,v.group);
            }
            context.write(new LongWritable(m), new Vertex(0,m,VID.get(),adj));
        }
    }

    public static class GraphFinalMapper extends Mapper<LongWritable,Vertex,LongWritable,LongWritable>{
 
        public void map (LongWritable group, Vertex value, Context context )
                        throws IOException, InterruptedException{
            context.write(group, new LongWritable(1));
        }
    }

    public static class GraphFinalReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
 
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context)
                           throws IOException, InterruptedException {
            long m=0;
            for(LongWritable v:values)
            {
                 m=m+v.get();
            }
            context.write(key,new LongWritable(m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job joba = Job.getInstance();
        joba.setJobName("MyJoba");
        joba.setJarByClass(Graph.class);
        joba.setOutputKeyClass(LongWritable.class);
        joba.setOutputValueClass(Vertex.class);
        joba.setMapOutputKeyClass(LongWritable.class);
        joba.setMapOutputValueClass(Vertex.class);
        joba.setMapperClass(GraphFirstMapper.class);
        joba.setInputFormatClass(TextInputFormat.class);
        joba.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(joba,new Path(args[0]));
        FileOutputFormat.setOutputPath(joba,new Path(args[1]+"/f0"));
        joba.waitForCompletion(true);
        for ( short i = 0; i < 5; i++ ) {
            Job jobb = Job.getInstance();
            jobb.setJobName("MyJobb");
            jobb.setJarByClass(Graph.class);
            jobb.setOutputKeyClass(LongWritable.class);
            jobb.setOutputValueClass(Vertex.class);
            jobb.setMapOutputKeyClass(LongWritable.class);
            jobb.setMapOutputValueClass(Vertex.class);
            jobb.setMapperClass(GraphSecondMapper.class);
            jobb.setReducerClass(GraphSecondReducer.class);
            jobb.setInputFormatClass(SequenceFileInputFormat.class);
            jobb.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(jobb,new Path(args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(jobb,new Path(args[1]+"/f"+(i+1)));
            jobb.waitForCompletion(true);
        }
        Job jobc = Job.getInstance();
        jobc.setJobName("MyJobc");
        jobc.setJarByClass(Graph.class);
        jobc.setOutputKeyClass(LongWritable.class);
        jobc.setOutputValueClass(Vertex.class);
        jobc.setMapOutputKeyClass(LongWritable.class);
        jobc.setMapOutputValueClass(LongWritable.class);
        jobc.setMapperClass(GraphFinalMapper.class);
        jobc.setReducerClass(GraphFinalReducer.class);
        jobc.setInputFormatClass(SequenceFileInputFormat.class);
        jobc.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobc,new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(jobc,new Path(args[2]));
        jobc.waitForCompletion(true);
    }
}