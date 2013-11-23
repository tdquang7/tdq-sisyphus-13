/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sisyphus;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author tdquang
 */
public class GraphNodeOutputFormat extends FileOutputFormat<Text, GraphNodeInfo>{
    @Override
    public RecordWriter<Text, GraphNodeInfo> getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {
        Configuration conf = tac.getConfiguration();
        
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(tac);
        Path path = new Path(committer.getWorkPath(), getUniqueFile(tac, "part", ""));
        
        FileSystem fs = path.getFileSystem (conf);
        FSDataOutputStream fileOut = fs.create (path, false);
        
        return new GraphNodeRecordWriter(fileOut);
    }
}
