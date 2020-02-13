package DB;
//mvn exec:java -Dexec.mainClass="DB.cassandraM"
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.DriverTimeoutException;

class cassandraM extends DBMain{

	private CqlSession session;
	private String table = "measurements";
	private String keyspace = "measurement";
	
	public void dbinit(){
		session = CqlSession.builder().build();
		session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
		session.execute("CREATE KEYSPACE " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		session.execute("USE " + keyspace);
		System.out.println("cassandra initialisiert");
	}
	
	public void dbconnect(){
		session = CqlSession.builder().build();
		session.execute("USE " + keyspace);
		System.out.println("cassandra verbunden");
	}
	
	public void dbdrop(){
		try{
			session.execute("DROP TABLE IF EXISTS " + table);
		}catch(DriverTimeoutException e){
			System.out.println("Timeout drop");
		}
		try{
		session.execute("CREATE TABLE " + table + "(key text PRIMARY KEY, data text);");
		}catch(DriverTimeoutException e){
			System.out.println("Timeout create");
		}
		System.out.println("Tabelle neu erstellt");
	}
	
	public String dbname(){
			return "Cassandra";
	}
	
	public void dbfill(String key, String data){
		session.execute("INSERT INTO " + table + " (key, data) VALUES ('" + key + "', '" + data + "');");
	}
	
	public void dbupdate(String key, String data){
		session.execute("UPDATE " + table + " SET data = '" + data + "' WHERE key = '" + key + "';");
	}
	
	public void dbdelete(String key){
		session.execute("DELETE FROM " + table + " WHERE key = '" + key + "' IF EXISTS;");
	}
	
	public int dbsize(){
		return session.execute("SELECT data FROM " + table).all().size();
	}

	public String dbget(String key){
		try{
		return session.execute("SELECT data FROM " + table + " WHERE key = '" + key + "'").one().getString(0);
		}catch(NullPointerException e) {return "";}
	}
	
	public void dbclose(){
		session.close();
		System.out.println("Cassandra wurde beendet");
	}
	
	public static void main(String[] args) {
		cassandraM d = new cassandraM();
		d.call();
	}
}