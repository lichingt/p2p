package comp90015.idxsrv.message;

import java.util.ArrayList;

import comp90015.idxsrv.server.IndexElement;

@JsonSerializable
public class LookupReply extends Message {

	@JsonElement
	public IndexElement[] hits;
	
	public LookupReply() {
		
	}
	
	public LookupReply(ArrayList<IndexElement> hits) {
		this.hits = new IndexElement[hits.size()];
		for(int i=0;i<hits.size();i++) {
			this.hits[i]=hits.get(i);
		}
	}
	
	
}
