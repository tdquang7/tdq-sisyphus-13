/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sisyphus;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.crypto.dsig.spec.C14NMethodParameterSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author tdquang
 */
public class Sisyphus {
    public static class SisyphusMapper extends Mapper <Text, GraphNodeInfo, Text, GraphNodeInfo> {         
        static CoreFunction f;

        @Override
        protected void setup(Context context)
        {            
            Class c = null;
            try {
                c = Class.forName(context.getConfiguration().get("coreF"));
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(Sisyphus.class.getName()).log(Level.SEVERE, null, ex);
            }
                
            try {
                f = (CoreFunction) c.newInstance() ;
            } catch (InstantiationException ex) {
                Logger.getLogger(Sisyphus.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Sisyphus.class.getName()).log(Level.SEVERE, null, ex);
            }            
        }        
        
        @Override
        protected void map (Text vertexID, GraphNodeInfo node, Context context) throws IOException, InterruptedException {
            f.SetInitialState(node);
            context.write(vertexID, node); // The graph structure            
            
            ArrayList<KeyValue> messages = f.ExtractMessages(vertexID, node);
            
            if (!messages.isEmpty())
            {
                for(KeyValue kv: messages)
                {
                    context.write(kv.key, kv.value);
                }
            }
	}
    }
    
    public static class SisyphusReducer extends Reducer<Text, GraphNodeInfo, Text, GraphNodeInfo>{        
       static CoreFunction f;
        
        @Override
        protected void setup(Context context)
        {
            Class c = null;
            try {
                c = Class.forName(context.getConfiguration().get("coreF"));
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(Sisyphus.class.getName()).log(Level.SEVERE, null, ex);
            }
                
            try {
                f = (CoreFunction) c.newInstance() ;
            } catch (InstantiationException ex) {
                Logger.getLogger(Sisyphus.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Sisyphus.class.getName()).log(Level.SEVERE, null, ex);
            }
        } 
        
        @Override
	protected void reduce (Text key, Iterable <GraphNodeInfo> messages, Context context) throws IOException, InterruptedException {
            GraphNodeInfo node = new GraphNodeInfo();
            f.SetInitialState(node);
            node.setVertexID(key.toString());
            
            
//            // Test coi chuyen gi da xay ra, dung identity
//            for(GraphNodeInfo f: messages)
//            {
//                context.write(key, f);
//            }
//            
            
            // Accumulate messages
            ArrayList<String> messInfos = new ArrayList();
            
            for(GraphNodeInfo info: messages)
            {
                if (info.IsMessage())
                {                
                    messInfos.add(info.getState());
                }
                else
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
            
            Object newState = f.CombineInfos(messInfos, key, node, context);            
            if (newState != null)
                f.UpdateState(node, newState);
           
            context.write(key, node);
	}
    } 
    
    // Helper class
    public static class KeyValue
    {
        private Text key;
        private GraphNodeInfo value;
        
        public Text getKey() { return key; }
        public void setKey(Text newKey) { key = newKey;}
        
        public GraphNodeInfo getValue() { return value; }
        public void setValue(GraphNodeInfo newValue) { value = newValue; }
    }
    
    // The core functions of framworks 
    public interface CoreFunction
    {
        void SetInitialState(GraphNodeInfo info);
        ArrayList<KeyValue> ExtractMessages(Text key, GraphNodeInfo info);   
        Object Aggregate(Object x, Object y);
        void UpdateState(GraphNodeInfo info, Object newState);
        Object CombineInfos(ArrayList<String> messInfos, Text key, GraphNodeInfo node, Context context);
    }
            
    public void RunAlgo(String[] args, String algoName, Class mainClass, Class coreF) throws Exception
    {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        Configuration config = new Configuration();
        config.set("coreF", coreF.getName());
        Job job = new Job(config, algoName);   
        job.setJarByClass(mainClass); 
        job.setMapperClass(SisyphusMapper.class);
        job.setReducerClass(SisyphusReducer.class);
      
        job.setInputFormatClass(GraphNodeInputFormat.class);
        job.setOutputFormatClass(GraphNodeOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNodeInfo.class);
        
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
                
        job.waitForCompletion(true);
    }
}

// DIDN't use

//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException
//        {
//            Set<String> keys = cache.keySet();
//            
//            for(String key: keys)
//            {
//                GraphNodeInfo value = (GraphNodeInfo)cache.get(key);
//                context.write(new Text(key), value);
//            }
//        }


// COmbine in context of map

//                if (kv.value.IsMessage()){
//                    String key = kv.key.toString();
//
//                    if (cache.contains(key)){
//                        GraphNodeInfo oldMessage = (GraphNodeInfo) cache.get(key);                       
//                        Object newValue = f.Aggregate(oldMessage.getState(), kv.value.getState());
//
//                        oldMessage.setState((String) newValue);
//
//                        cache.put(key, oldMessage);
//                    }
//                    else
//                    {
//                        cache.put(key, kv.value); // String, GraphNodeInfo
//                    }
//                }
//                else // Is graph strcucture 
//                {                        
//                    context.write(kv.key, kv.value);
//                }