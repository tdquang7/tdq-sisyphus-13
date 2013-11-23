/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sisyphus;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author tdquang
 */
public class GraphNodeRecordWriter extends RecordWriter<Text, GraphNodeInfo>{
    private Writer writer;
    
    public GraphNodeRecordWriter(DataOutputStream output) throws IOException {
        this.writer = new OutputStreamWriter(output, "UTF-8");    
    }        
    
    @Override
    public synchronized void write(Text key, GraphNodeInfo value) throws IOException { 
        final String LINEBREAK = "\n";
        
        writer.write(key.toString() + LINEBREAK); 
        
        if (value.IsMessage())
            writer.write("Message" + LINEBREAK);
        
        writer.write(value.getState() + LINEBREAK);

        ArrayList<String> edges = value.getEdges();
        ArrayList<String> infos = value.getInfo();

        writer.write(Integer.toString(edges.size()) + LINEBREAK);

        for (int i = 0; i < edges.size(); i++){
            String link = edges.get(i);
            String info = infos.get(i);

            writer.write(link);

            if (info.length() > 0)
                writer.write(", " + info);

            writer.write(LINEBREAK);
        }
        
        writer.write(LINEBREAK);
    }

    @Override
    public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
        writer.flush();
        writer.close();
    }
}
