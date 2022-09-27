package comp90015.idxsrv.message;

@JsonSerializable
public class SearchRequest extends Message {

	@JsonElement
	public Integer maxhits;
	
	@JsonElement
	public String[] keywords;
	
	public SearchRequest() {
		
	}
	
	public SearchRequest(Integer maxhits, String[] keywords) {
		this.maxhits=maxhits;
		this.keywords=keywords;
	}
	
}
