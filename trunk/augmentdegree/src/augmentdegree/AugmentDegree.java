/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package augmentdegree;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author tdquang
 */
public class AugmentDegree {
    public static class AdMapper1 extends Mapper<Text, GraphNodeInfo, Text, GraphNodeInfo>
    {       
        @ Override
	protected void map (Text vertexID, GraphNodeInfo node, Context context) throws IOException, InterruptedException {            
            // 1. Set initial state: Doing nothing
            context.write(vertexID, node); // 2. Release the graph structure    
            
            // 3. Extract messages
            for(String edge: node.getEdges())
            {
                GraphNodeInfo message = new GraphNodeInfo();
                message.setVertexID(edge); // Vertex needed to be added degree
                message.setTypeIsMessage();
                message.setState(edge + ", " + vertexID.toString());
                
                // 4. Release all these messages
                context.write(new Text(edge), message);
            }
	}
    }
    
    public static class AdReducer1 extends Reducer<Text, GraphNodeInfo, Text, GraphNodeInfo>{
        @Override
	protected void reduce (Text key, Iterable <GraphNodeInfo> messages, Context context) 
                throws IOException, InterruptedException {
            
            GraphNodeInfo node = new GraphNodeInfo();
            // 1. Set initial state: none
            node.setVertexID(key.toString());
            
            ArrayList<String> messInfos = new ArrayList();
            
            for (GraphNodeInfo info: messages)
            {
                if (info.IsMessage()) 
                {
                    messInfos.add(info.getState()); 
                }
                else // is the vertex
                {
                     // Extracting out by copy node infomation
                    node.setState(info.getState());
                    ArrayList<String> edges = info.getEdges();
                    ArrayList<String> infos = info.getInfo();
                    
                    for(int i = 0; i < edges.size(); i++)
                    {
                        node.getEdges().add(edges.get(i));
                        node.getInfo().add(infos.get(i));
                    }
                }
            }
            
            // Combine infos            
            for(String data: messInfos)
            {
                String[] parts = data.split(", ");
                String v = parts[1];
                
                GraphNodeInfo getM = new GraphNodeInfo();
                getM.setVertexID(v);
                
                String nodeDegree = Integer.toString(node.getEdges().size());
                getM.setState(key.toString() + ", " + nodeDegree);
                getM.setTypeIsMessage();
                
                context.write(new Text(v), getM);
            }
            
            context.write(key, node); // Release the graph structure            
	}
    }    
    
    public static class KeyValue
    {
        private Text key;
        private GraphNodeInfo value;
        
        public Text getKey() { return key; }
        public void setKey(Text newKey) { key = newKey;}
        
        public GraphNodeInfo getValue() { return value; }
        public void setValue(GraphNodeInfo newValue) { value = newValue; }
    }
    
    public static class AdMapper2 extends Mapper<Text, GraphNodeInfo, Text, GraphNodeInfo>
    {       
        // Identity function
        @ Override
	protected void map (Text vertexID, GraphNodeInfo node, Context context) throws IOException, InterruptedException {            
            context.write(vertexID, node); // Identity function
	}
    }
    
    public static class AdReducer2 extends Reducer<Text, GraphNodeInfo, Text, GraphNodeInfo>{
        @Override
	protected void reduce (Text key, Iterable <GraphNodeInfo> messages, Context context) throws IOException, InterruptedException {
            GraphNodeInfo node = new GraphNodeInfo();
            // 1. Set initial state: none
            node.setVertexID(key.toString());
            
            // Find the node
            ArrayList<String> weights = new ArrayList();
            
            for (GraphNodeInfo info: messages)
            {
                if (info.IsMessage()) 
                {
                    weights.add(info.getState());
                }
                else // is the vertex
                {
                    ArrayList<String> es = info.getEdges();
                    ArrayList<String> is = info.getInfo();
                    
                    for(int i = 0; i < es.size(); i++)
                    {
                        node.getEdges().add(es.get(i));
                        node.getInfo().add(is.get(i));
                    }
                }
            }
            
            // Update the node state
            for(int i = 0; i < weights.size(); i++)
            {                     
                String[] parts = weights.get(i).split(", ");                
                
                for(int j = 0; j < node.getEdges().size(); j++)
                {
                    if (node.getEdges().get(j).equalsIgnoreCase(parts[0]))
                    {
                        node.getInfo().set(j, parts[1]);
                        
                        break;
                    }
                }
            }
            
            context.write(key, node); // Release the graph structure
	}
    }    
       
    public static void main(String[] args) throws Exception{   
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
    
        Configuration conf = new Configuration();        
    
        Job job = new Job(conf, "AugmentDegree1");
        job.setJarByClass(AugmentDegree.class);
        job.setMapperClass(AdMapper1.class);
        job.setReducerClass(AdReducer1.class);
                
        job.setInputFormatClass(GraphNodeInputFormat.class);
        job.setOutputFormatClass(GraphNodeOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNodeInfo.class);
        
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
                
        job.waitForCompletion(true);              
        //----------------------------------------
        
        inPath = new Path(args[1]);
        outPath = new Path(args[1] + "1");
        
        job = new Job(conf, "AugmentDegree2");
        job.setJarByClass(AugmentDegree.class);
        job.setMapperClass(AdMapper2.class);
        job.setReducerClass(AdReducer2.class);
                
        job.setInputFormatClass(GraphNodeInputFormat.class);
        job.setOutputFormatClass(GraphNodeOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNodeInfo.class);
        
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
                
        job.waitForCompletion(true);   
    }   
}
