/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pagerank;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.ArrayList;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author tdquang
 */
public class GraphNodeRecordReader extends RecordReader<Text, GraphNodeInfo>{
    private int count = 0; 
    private FileSystem fs = null;
    private FSDataInputStream input = null;
    private LineReader reader = null;
    
    private Text currentKey = null;
    private GraphNodeInfo currentValue = null;
    private long fileSize;
    private int totalRecords;
    
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
            
            if ("".equals(vertexID))
            {
                currentKey = null;
                currentValue = null;
                return false;
            }
            
            currentValue = new GraphNodeInfo();
            currentValue.setVertexID(vertexID);
            
            reader.readLine(line);
            String s = line.toString();
            if (!s.isEmpty())
                currentValue.setPR(Float.parseFloat(line.toString()));
            
            reader.readLine(line);
            String number = line.toString();
            int edgesCount = (number.length() == 0)? 0 : Integer.parseInt(number);
            
            for(int i = 0; i < edgesCount; i++)
            {
                reader.readLine(line);
                
                currentValue.getEdges().add(line.toString());
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
