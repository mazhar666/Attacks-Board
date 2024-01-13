package Events;

import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.Setter;

import java.util.UUID;
@Data
@AllArgsConstructor
@Builder
public class Event {
    public  String mainCategory;
    public  String subCategory;
    public  String protocol;
    public  String sourcePort;

    public  String destinationPort;
    public  String name;
    public  UUID id ;
    public String sourceIP;
    @Setter
    public String severity;
    @Setter
    public boolean authorizedIp=true;
    public  Event(){}
    public Event(String[] props){
        id= UUID.randomUUID();
        mainCategory=props[0];
        subCategory=props[1];
        protocol=props[2];
        sourceIP=props[3];
        sourcePort=props[4];
        destinationPort=props[5];
        name=props[6];
    }

}
