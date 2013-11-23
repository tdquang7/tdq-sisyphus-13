/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sisyphus;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author tdquang
 * 
 */
public class GraphNodeRecordReader extends RecordReader<Text, GraphNodeInfo>{
    private int count = 0; 
    private FileSystem fs = null;
    private FSDataInputStream input = null;
    private LineReader reader = null;
    
    private Text currentKey = null;
    private GraphNodeInfo currentValue = null;
    private long fileSize;
        
    public GraphNodeRecordReader(Configuration conf, FileSplit split) throws IOException {
        this.fs = FileSystem.get(conf);
        
        Path path = split.getPath();
        fileSize = fs.getFileStatus(path).getLen();        
        input = fs.open(path);
        
        if ((input.getPos() < fileSize) && (fileSize > 0))
        {
            reader = new LineReader(input);   
            fileSize = fs.getFileStatus(path).getLen();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (fileSize == 0 )
            return false;
        
        boolean foundRecord = false;
        currentKey = null;
        currentValue = null;
                
        Text line = new Text();
        
        int size = reader.readLine(line);
        
        if (size != 0) // Found the first line of next record
        {
            foundRecord = true;
           
            String vertexID = line.toString();
            currentKey = new Text(vertexID);
            
            if (vertexID.isEmpty())
            {
                currentKey = null;
                currentValue = null;
                return false;
            }
            
            currentValue = new GraphNodeInfo(); // ID of the vertex
            currentValue.setVertexID(vertexID);
            
            reader.readLine(line); // Read the state
            String state = line.toString();
            
            if (state.equalsIgnoreCase("message"))
            {
                currentValue.setTypeIsMessage();
                
                reader.readLine(line); // Read the  again
                state = line.toString();
            }            
            
            currentValue.setState(state);  
            
            reader.readLine(line); // number of edges
            String number = line.toString();
            int edgesCount = (number.length() == 0)? 0 : Integer.parseInt(number);
            
            for(int i = 0; i < edgesCount; i++)
            {
                reader.readLine(line);
                String[] parts = line.toString().split(", ");
                String edge = line.toString();
                String info = ""; // The first reading time there is no info for the edge
                
                if (parts.length > 1)
                {
                    edge = parts[0];
                    info = parts[1];
                }
                
                currentValue.AddEdgeWithInfo(edge, info);
            }            
        }
        
        reader.readLine(line); // Bo qua mot dong trong
        
        if (foundRecord)
            count++;

        return foundRecord;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public GraphNodeInfo getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }
    
    @Override
    public float getProgress() throws IOException {
        return input.getPos() / fileSize * 100;
    }

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        
    }
    
    @Override
    public void close() throws IOException {
        
    }
}
