package pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.WritableComparable;
import sun.security.x509.IssuerAlternativeNameExtension;

/**
 *
 * @author tdquang
 */
public class GraphNodeInfo implements WritableComparable {
    private String vertexID;
    private boolean isMessage;
    private Float PR;
    private ArrayList<String> edgeList;
    
    //*** Properties
    public String getVertexID() {return vertexID;}
    public boolean IsMessage() {return isMessage;}
    public Float getPR() {return PR;}
    public void setPR(Float newPR) {PR = newPR;}
    public void setVertexID(String id) { vertexID = id; }
    public ArrayList<String> getEdges() {return edgeList;}    
    
    public GraphNodeInfo(){
        vertexID = "";
        isMessage = false;
        PR = 1.0f;
        edgeList = new ArrayList();
    }
    
    public void setTypeIsMessage()
    {
        isMessage = true;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(vertexID);
        out.writeBoolean(isMessage);
        out.writeFloat(PR);
        
        out.writeInt(edgeList.size());
        
        for (int i = 0; i < edgeList.size(); i++)        
        {
            String edge = edgeList.get(i);
            out.writeUTF(edge);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        edgeList = new ArrayList();
        
        vertexID = in.readUTF();
        isMessage = in.readBoolean();
        PR = in.readFloat();
        
        int edgesCount = in.readInt();
        
        for (int i = 0; i < edgesCount; i++)
        {
            String edge =  in.readUTF();
            edgeList.add(edge);
        }
    }

    @Override
    public String toString() {
        String info = "";
        info += edgeList.toString();
        
        return info;
    }
    
    @Override
    public int compareTo(Object o) {
        GraphNodeInfo other = (GraphNodeInfo)o;
        return this.vertexID.compareTo(other.vertexID);
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GraphNodeInfo)) {
            return false;
        }

        GraphNodeInfo other = (GraphNodeInfo)o;
        
    return this.vertexID.equals(other.vertexID);
  }

    @Override
  public int hashCode() {
    return vertexID.toString().hashCode();
  }
}
