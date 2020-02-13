package DB;

//mvn exec:java -Dexec.mainClass="DB.mongoM"
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;

class mongoM extends DBMain{

	public String dbName = "measurement";
	public String collectionName = "measurementCollection";
	public MongoClient mongoClient;
	public MongoDatabase database;
	public MongoCollection<Document>  collection;

	public void dbinit(){
		mongoClient = MongoClients.create();
		mongoClient.getDatabase(dbName).drop();
		database = mongoClient.getDatabase(dbName);
		System.out.println("mongo initialisiert");
	}

	public void dbconnect(){
		mongoClient = MongoClients.create();
		collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
		System.out.println("mongo verbunden");
	}
	
	public void dbdrop(){
		database.getCollection(collectionName).drop();
		collection = database.getCollection(collectionName);
		System.out.println("mongo Collection neu erstellt");
	}
	
	public String dbname(){
			return "mongoDB";
	}
	
	public void dbfill(String key, String data) {
		Document entry = new Document("_id", key).append("data", data);
		collection.insertOne(entry);
	}
	
	public String dbget(String key){
		try{
		Document cursor = collection.find(eq("_id", key)).first();
		return (String)cursor.get("data");
		}catch(NullPointerException e) {return "";}
	}
	
	public void dbupdate(String key, String data){
		collection.updateOne(eq("_id", key), new Document("$set", new Document("data", data)));
	}
	
	public void dbdelete(String key){
		try{
		collection.deleteOne(eq("_id", key));
		}catch(NullPointerException e) {}
	}
	
	public int dbsize(){
		return (int)collection.countDocuments();
	}
	
	public void dbclose(){
		mongoClient.close();
		System.out.println("mongoDB wurde beendet");
	}
	
	public static void main(String[] args) {
		mongoM d = new mongoM();
		d.call();
	}
}
