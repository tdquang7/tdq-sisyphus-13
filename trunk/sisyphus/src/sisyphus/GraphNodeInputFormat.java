package tdq.sisyphus;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GraphNodeInputFormat extends FileInputFormat<Text, GraphNodeInfo> 
{
    // Ensure that we cannot split the input!!!
    @Override
    protected boolean isSplitable(JobContext context,Path filename)
    {
        return false;
    }
            
    @Override
    public RecordReader<Text, GraphNodeInfo> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException 
    { 
        return new GraphNodeRecordReader(tac.getConfiguration(), (FileSplit) is);
    }
}
