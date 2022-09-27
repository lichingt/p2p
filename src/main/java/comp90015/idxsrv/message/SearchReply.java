package comp90015.idxsrv.message;

import java.util.ArrayList;

import comp90015.idxsrv.server.IndexElement;

@JsonSerializable
public class SearchReply extends Message {

	@JsonElement
	public IndexElement[] hits;
	
	@JsonElement
	public Integer[] seedCounts;
	
	public SearchReply() {
		
	}
	
	public SearchReply(ArrayList<IndexElement> hits, Integer[] seedCounts) {
		this.hits = new IndexElement[hits.size()];
		this.seedCounts = seedCounts;
		for(int i=0;i<hits.size();i++) {
			this.hits[i]=hits.get(i);
		}
	}
	
	
}
