/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package saugmentdegree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import sisyphus.GraphNodeInfo;
import sisyphus.Sisyphus;
import sisyphus.*;
/*
 * @author tdquang
 * Implement pagerank using proposed framework Sisyphus
 */
public class Saugmentdegree extends Sisyphus{
    public static class ADCore1 implements Sisyphus.CoreFunction
    {
        @Override
        public Object Aggregate(Object x, Object y) {
             return null;             
        }

        @Override
        public ArrayList<KeyValue> ExtractMessages(Text key, GraphNodeInfo node) {
            
            ArrayList<KeyValue> messages = new ArrayList();
            
            for(String edge: node.getEdges())
            {
                GraphNodeInfo message = new GraphNodeInfo();
                message.setVertexID(edge); // Vertex needed to be added degree
                message.setTypeIsMessage();
                message.setState(edge + ", " + key.toString());
             
                KeyValue kv = new KeyValue();
                kv.setKey(new Text(edge));
                kv.setValue(message);
                
                messages.add(kv);
            }
            
            return messages;
        }

        @Override
        public void UpdateState(GraphNodeInfo info, Object newState) {    
            // Nothing to do
        }       
        
        @Override
        public void SetInitialState(GraphNodeInfo info)
        {
            // Do nothing
        }
        
        @Override
        public Object CombineInfos(ArrayList<String> messInfos, Text key, GraphNodeInfo node, Context context) 
        {
            for(String data: messInfos)
            {
                String[] parts = data.split(", ");
                String v = parts[1];

                GraphNodeInfo getM = new GraphNodeInfo();
                getM.setVertexID(v);

                String nodeDegree = Integer.toString(node.getEdges().size());
                getM.setState(key.toString() + ", " + nodeDegree);
                getM.setTypeIsMessage();

                try {
                    context.write(new Text(v), getM);
                } catch ( IOException | InterruptedException ex) {
                    Logger.getLogger(Saugmentdegree.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            
            return null;
        }
    }
    
    public static class ADCore2 implements Sisyphus.CoreFunction
    {
        @Override
        public Object Aggregate(Object x, Object y) {
             return null;             
        }

        @Override
        public ArrayList<Sisyphus.KeyValue> ExtractMessages(Text key, GraphNodeInfo info) {
            ArrayList<KeyValue> list = new ArrayList();
            
            return list;
        }

        @Override
        public void UpdateState(GraphNodeInfo node, Object newState) {    
            
            ArrayList<String> weights = (ArrayList<String>) newState;
            
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
        }       
        
        @Override
        public void SetInitialState(GraphNodeInfo info)
        {
            // Nothing to do
        }
        
        @Override
        public Object CombineInfos(ArrayList<String> messInfos, Text key, GraphNodeInfo node, Context context)
        {            
            return messInfos;
        }
    }
    
    public static void main(String[] args) throws Exception{ 
        Sisyphus framework = new Sisyphus();
        framework.RunAlgo(args, "AD1", Sisyphus.class, ADCore1.class);
        
        String[] newoutput = new String[2];
        newoutput[0] = args[1];
        newoutput[1] = args[1] + "1";
        
        framework = new Sisyphus();
        framework.RunAlgo(newoutput, "AD2", Sisyphus.class, ADCore2.class);
    }
}