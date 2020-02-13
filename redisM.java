package DB;
//mvn exec:java -Dexec.mainClass="DB.redisM"
import redis.clients.jedis.Jedis;
import java.util.Set;

class redisM extends DBMain{

	private Jedis jedis;
	
	public void dbinit(){
		jedis = new Jedis("localhost", 6379, 0);
		System.out.println("Redis initialisiert");
	}
	
	public void dbconnect(){
		jedis = new Jedis("localhost", 6379, 0);
		System.out.println("Redis verbunden");
	}
	
	public void dbdrop(){
		jedis.flushDB();
		System.out.println("Datenbank geleert");
	}
	
	public String dbname(){
			return "Redis";
	}
	
	public void dbfill(String key, String data){
		jedis.set(key, data);
	}
	
	public void dbupdate(String key, String data){
		jedis.set(key, data);
		
	}
	
	public void dbdelete(String key){
		jedis.del(key);
	}
	
	public int dbsize(){
		return jedis.keys("*").size();
	}
	
	public String dbget(String key){
		return jedis.get(key);
	}
	
	public void dbclose(){
		jedis.close();
		System.out.println("Redis wurde beendet");
	}
	
	public static void main(String[] args) {
		redisM d = new redisM();
		d.call();
	}
}