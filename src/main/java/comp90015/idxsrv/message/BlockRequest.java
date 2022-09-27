package comp90015.idxsrv.message;

@JsonSerializable
public class BlockRequest extends Message{

    @JsonElement
    public String fileMd5;

    @JsonElement
    public String filename;

    @JsonElement
    public Integer blockIdx;

    public BlockRequest(){

    }

    public BlockRequest(String filename, String fileMd5, Integer blockIdx){
        this.filename = filename;
        this.fileMd5 = fileMd5;
        this.blockIdx = blockIdx;
    }

}
