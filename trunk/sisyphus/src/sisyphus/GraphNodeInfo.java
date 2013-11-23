package tdq.sisyphus;

import com.sun.org.apache.xml.internal.utils.StringComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.WritableComparable;
import sun.security.x509.IssuerAlternativeNameExtension;

public class GraphNodeInfo implements WritableComparable {
    private String vertexID;
    private boolean isMessage;
    private String state;
    private ArrayList<String> edgeList;
    private ArrayList<String> infoList;
    
    //*** Properties
    public String getVertexID() {return vertexID;}
    public boolean IsMessage() {return isMessage;}
    public String getState() {return state;}
    public void setState(String newState) {state = newState;}
    public void setVertexID(String id) { vertexID = id; }
    public ArrayList<String> getEdges() {return edgeList;}    
    public ArrayList<String> getInfo() {return infoList;}
    
    public void AddEdgeWithInfo(String edge, String info) {edgeList.add(edge); infoList.add(info);}   
        
    public GraphNodeInfo(){
        isMessage = false;
        state = "";
        edgeList = new ArrayList();
        infoList = new ArrayList();
    }
    
    public void setTypeIsMessage()
    {
        isMessage = true;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(vertexID);
        out.writeBoolean(isMessage);
        out.writeUTF(state);
        
        out.writeInt(edgeList.size());
        
        for (int i = 0; i < edgeList.size(); i++)        
        {
            String edge = edgeList.get(i);
            String info = infoList.get(i);
            
            if (info != null)
            {
                if (info.length() > 0)
                    edge += ", " + info;
            }
            
            out.writeUTF(edge);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        edgeList = new ArrayList();
        infoList = new ArrayList();
        
        vertexID = in.readUTF();
        isMessage = in.readBoolean();
        state = in.readUTF();
        
        int edgesCount = in.readInt();
        
        for (int i = 0; i < edgesCount; i++)
        {
            String s =  in.readUTF();
            
            String[] parts = s.split(", ");
            
            if (parts.length > 1)
            {
                edgeList.add(parts[0]);
                infoList.add(parts[1]);
            }            
            else
            {
                edgeList.add(s);
                infoList.add("");
            }
        }
    }

    @Override
    public String toString() {
        String info = "";
        
        info += state.toString();
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
