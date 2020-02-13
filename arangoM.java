package DB;
//mvn exec:java -Dexec.mainClass="DB.arangoM"
import com.arangodb.ArangoDB;
import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.ArangoCursor;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.util.MapBuilder;
import java.util.Map;

class arangoM extends DBMain{

	public String dbName = "measurement";
	public String collectionName = "measurementCollection";
	public ArangoDB arangoDB;
	public ArangoCollection collection;

	public void dbinit(){
		arangoDB = new ArangoDB.Builder().build();
		if(arangoDB.db(dbName).exists()) {
			arangoDB.db(dbName).drop();
		}
		arangoDB.createDatabase(dbName);
		System.out.println("Arango initialisiert");
	}
	
	public void dbconnect(){
		arangoDB = new ArangoDB.Builder().build();
		collection = arangoDB.db(dbName).collection(collectionName);
		System.out.println("Arango verbunden");
	}
	
	public void dbdrop(){
		if(collection == null){
			collection = arangoDB.db(dbName).collection(collectionName);
			System.out.println("collection in variable");
		}
		if(collection.exists()) {
			collection.drop();
		}
		arangoDB.db(dbName).createCollection(collectionName);
		System.out.println("Arango Collection neu erstellt");
	}
	
	public String dbname(){
			return "ArangoDB";
	}
	
	public void dbfill(String key, String data) {
		myObject = new BaseDocument();
		myObject.setKey(key);
		myObject.addAttribute("data", data);
		collection.insertDocument(myObject);
	}
	
	public void dbupdate(String key, String data){
		myObject.setKey(key);
		myObject.updateAttribute("data", data);
		collection.updateDocument(key, myObject);
	}
	
	public void dbdelete(String key){
		if(collection.documentExists(key)){
			collection.deleteDocument(key);
		}
	}
	
	public int dbsize(){
		return collection.count().getCount().intValue();
	}
	
	public String dbget(String key){
		try{
		myObject = collection.getDocument(key, BaseDocument.class);
		return myObject.getAttribute("data").toString();
		}catch(NullPointerException e) {return "";}
		catch(ArangoDBException c){return "";}
	}
	
	public void dbclose(){
		arangoDB.shutdown();
		System.out.println("ArangoDB wurde beendet");
	}
	
	public static void main(String[] args) {
		arangoM d = new arangoM();
		d.call();
	}
}