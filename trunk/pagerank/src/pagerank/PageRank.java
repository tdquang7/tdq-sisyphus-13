/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pagerank;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author tdquang
 */
public class PageRank {    
    public static class PrMapper extends Mapper<Text, GraphNodeInfo, Text, GraphNodeInfo>
    {       
        @ Override
	protected void map (Text vertexID, GraphNodeInfo node, Context context) throws IOException, InterruptedException {
            node.setPR(1.0f);
            context.write(vertexID, node); // The graph structure             
                       
            Float messageToSend = node.getPR() / node.getEdges().size();
            for(String edge: node.getEdges())
            {
                GraphNodeInfo message = new GraphNodeInfo();
                message.setVertexID(vertexID.toString()); // Message from
                message.setTypeIsMessage();
                message.setPR(messageToSend);
                
                context.write(new Text(edge), message);
            }
	}
    }
    
    public static class PrReducer extends Reducer<Text, GraphNodeInfo, Text, GraphNodeInfo>{
        @Override
	protected void reduce (Text key, Iterable <GraphNodeInfo> messages, Context context) throws IOException, InterruptedException {
            GraphNodeInfo node = new GraphNodeInfo();
            node.setVertexID(key.toString());
            
            Float finalPR = 0.f;
            
            for(GraphNodeInfo info: messages)
            {
                if (info.IsMessage())                
                    finalPR += info.getPR();                
                else
                {                    
                    node.setPR(info.getPR());
                    for(String edge: info.getEdges())
                        node.getEdges().add(edge);
                }                    
            }
            
            node.setPR(node.getPR() + finalPR);
            
            context.write(key, node);
	}
    }    
       
    public static void main(String[] args) throws Exception{   
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PrMapper.class);
        job.setReducerClass(PrReducer.class);
                
        job.setInputFormatClass(GraphNodeInputFormat.class);
        job.setOutputFormatClass(GraphNodeOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNodeInfo.class);
        
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
                
        job.waitForCompletion(true);        
    }   
}