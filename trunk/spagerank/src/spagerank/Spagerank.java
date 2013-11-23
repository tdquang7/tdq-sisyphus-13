/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tdq.pr;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import tdq.sisyphus.GraphNodeInfo;
import tdq.sisyphus.Sisyphus;
import tdq.sisyphus.*;

/*
 * @author tdquang
 * Implement pagerank using proposed framework Sisyphus
 */
public class PageRankSisyphus extends Sisyphus{
    public static class PRCoreFunction implements CoreFunction
    {
        @Override
        public Object Aggregate(Object x, Object y) {
            Float a, b;
            
            String i = (String) x;
            String j = (String) y;
            if (x == null) a = 0.f;
            else if (i.isEmpty()) a = 0.f;
            else a = Float.parseFloat(i);
            
            if (y == null) b = 0.f;
            else if (j.isEmpty()) b = 0.f;
            else b = Float.parseFloat(j);
            
            Float result = a + b;
            return result.toString();                    
        }

        @Override
        public ArrayList<KeyValue> ExtractMessages(Text key, GraphNodeInfo info) {
            if (info.getState().equals(""))
            {                
                info.setState("1.0");
            }
            
            Float PR = Float.parseFloat(info.getState());            
            int size = info.getEdges().size();
            Float valuesToSend = PR / size;
            
            ArrayList<KeyValue> list = new ArrayList();
            
            // Create message from ajacencyList
            for(String edge: info.getEdges())
            {
                KeyValue kv = new KeyValue();
                kv.setKey(new Text(edge));
                
                // Using state to contain message to other node
                GraphNodeInfo tempNode = new GraphNodeInfo();
                tempNode.setVertexID(edge);
                tempNode.setTypeIsMessage();
                tempNode.setState(valuesToSend.toString());
                kv.setValue(tempNode);
               
                list.add(kv);
            }
            
            return list;
        }

        @Override
        public void UpdateState(GraphNodeInfo info, Object newState) {    
            Object oldState = info.getState();
            newState = Aggregate(oldState, newState);
            info.setState((String) newState);
        }       
        
        @Override
        public void SetInitialState(GraphNodeInfo info)
        {
            info.setState("1.0");
        }
        
        @Override
        public Object CombineInfos(ArrayList<String> messInfos, Text key, GraphNodeInfo node, Context context)
        {
            Object result = "";
            
            for(String info: messInfos)
            {
                result = Aggregate(info, result);
            }
            
            // Nothing to do with the context
            
            return result;
        }
    }
    
    public static void main(String[] args) throws Exception{ 
        Sisyphus framework = new Sisyphus();
        framework.RunAlgo(args, "PrAlgo", Sisyphus.class, PRCoreFunction.class);
    }
}
