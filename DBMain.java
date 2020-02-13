package DB;
//mvn exec:java -Dexec.mainClass="DB.DBMain"
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.*;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;

abstract class DBMain {

	public abstract void dbinit();
	public abstract void dbconnect();
	public abstract void dbdrop();
	public abstract String dbname();
	public abstract void dbfill(String key, String data);
	public abstract void dbupdate(String key, String data);
	public abstract void dbdelete(String key);
	public abstract int dbsize();
	public abstract String dbget(String key);
	public abstract void dbclose();

	public PrintWriter pWriter = null;
	public final Double mil = 1000000.0;
	public final Double sec = 1000000000.0;

	public void execute(boolean fill, ArrayList<Integer> keys, int from, int to, boolean random, int percent, boolean delete, boolean check){
		if(fill){
			
			for(int i = from; i < to; i++){
				dbfill(Integer.toString(keys.get(i)), Integer.toString(keys.get(i)));
			}
		}
		else{
			int key;
			String data = "";
			int db = dbsize();
			int limit;
			Random rand = new Random();
			for(int i = from; i < to; i++){
				if(random){
					key = rand.nextInt(db);
				}
				else{
					key = i % db;
				}
				if(percent > 0){
					limit = rand.nextInt(100);
					if(percent > limit){
						if(delete){
								dbdelete(Integer.toString(key));
						}
						else{
								dbupdate(Integer.toString(key), Integer.toString(key+1));
						}
					}
					else{
						data = dbget(Integer.toString(key));
					}
				}
				else{
					data = dbget(Integer.toString(key));
				}
				if(check){
					if(Integer.valueOf(data) != key){
						System.err.printf("Gelesenes entspricht nicht Key. Key: %s, Data: %s%n", key, data);
					}
				}
			}
		}
	}

	public Double measurement(int start, int end, boolean fill, boolean random, int percent, boolean delete, boolean check){
		System.out.printf("start: %s, end: %s, fill: %s, random: %s, percent: %s, delete: %s, check: %s%n", start, end, fill, random, percent, delete, check);
		long tstart;
		long tend;
		tstart  = System.nanoTime();
		ArrayList<Integer> keys = new ArrayList<Integer>();

		if(fill){
			for(int i = start; i < end; i++) {
				keys.add(i);
			}
			if(random){
				Collections.shuffle(keys);
			}
		}
		execute(fill, keys, start, end - start, random, percent, delete, check);
		tend = System.nanoTime();
		System.out.println("abgeschlossen");
		return (tend - tstart)/sec;
	}

	public void print(String title, Double[] data, Double[] calls, Double[] head){
		String[] text = new String[calls.length];
		text = DoubletoSting(head);
		text[0] = title;
		pWriter.println(String.join(", ", text));
		text = DoubletoSting(data);
		text[0] = "Absolut";
		pWriter.println(String.join(", ", text));
		pWriter.println(String.join(", ", normspro1M(data, calls)));
		pWriter.println(String.join(", ", normApros(data, calls)));
		pWriter.println("\n");
	}

	public String[] DoubletoSting(Double[] in){
		String[] out = new String[in.length];
		for(int i=1; i < in.length; i++){
			out[i] = Double.toString(in[i]);
		}
		return out;
	}

	public String[] normspro1M(Double[] measurement, Double[] calls) {
		String[] text = new String[measurement.length];
		text[0] = "Sekunden pro 1Millionen Befehle";
		for(int i = 1; i < calls.length; i++){
			text[i] = Double.toString(measurement[i] * mil / calls[i]);
		}
		return text;
	}

	public String[] normApros(Double[] measurement, Double[] calls) {
		String[] text = new String[measurement.length];
		text[0] = "Befehle pro Sekunde";
		for(int i = 1; i < calls.length; i++){
			text[i] = Double.toString(calls[i]/ measurement[i]);
		}
		return text;
	}

	public void crs(){
		pWriter.println("compare random-sorted depending on databasesize, calls 1000000");
		Double[] calls = new Double[]{0.0, 50000.0, 100000.0, 500000.0, 1000000.0, 2000000.0, 3000000.0, 4000000.0, 5000000.0};
		int len = calls.length;
		Double[] timefill = new Double[len];
		Double[] timereads = new Double[len];
		Double[] timereadr = new Double[len];
		Double[] callfill = new Double[len];
		Double[] callread = new Double[len];
		Random rand = new Random();
		int shift;
		for(int i=1; i<len; i++){
			dbdrop();
			timefill[i] = measurement(0, calls[i].intValue(), true, false, 0, false, false);
			callfill[i] = calls[i];
			timereadr[i] = measurement(0, mil.intValue(), false, true, 0, false, true);
			shift = rand.nextInt(calls[i].intValue());
			timereads[i] = measurement(shift, mil.intValue()+shift, false, false, 0, false, true);
			callread[i] = mil;
		}

		print("Einfugen Sortiert", timefill, callfill, calls);
		print("Lesen Random Sortiert", timereadr, callread, calls);
		print("Lesen Sortiert Sortiert", timereads, callread, calls);

		for(int i=1; i<len; i++){
			dbdrop();
			timefill[i] = measurement(0, calls[i].intValue(), true, true, 0, false, false);
			timereadr[i] = measurement(0, mil.intValue(), false, true, 0, false, true);
			shift = rand.nextInt(calls[i].intValue());
			timereads[i] = measurement(shift, mil.intValue()+shift, false, false, 0, false, true);
		}

		print("Einfugen Random", timefill, callfill, calls);
		print("Lesen Random Random", timereadr, callread, calls);
		print("Lesen Sortiert Random", timereads, callread, calls);
	}

	public void crsn(){
		pWriter.println("compare random-sorted depending on calls, databasesize 1000000");
		Double[] calls = new Double[]{0.0, 50000.0, 100000.0, 500000.0, 1000000.0, 2000000.0, 3000000.0, 4000000.0, 5000000.0};
		int len = calls.length;
		Double[] timereads = new Double[len];
		Double[] timereadr = new Double[len];
		Random rand = new Random();
		int shift;
		dbdrop();
		measurement(0, mil.intValue(), true, false, 0, false, false);
		for(int i=1; i<len; i++){
			timereadr[i] = measurement(0, calls[i].intValue(), false, true, 0, false, true);
			shift = rand.nextInt(calls[i].intValue());
			timereads[i] = measurement(shift, calls[i].intValue()+shift, false, false, 0, false, true);
		}

		print("Lesen Random Sortiert", timereadr, calls, calls);
		print("Lesen Sortiert Sortiert", timereads, calls, calls);

		dbdrop();
		measurement(0, mil.intValue(), true, true, 0, false, false);
		for(int i=1; i<len; i++){
			timereadr[i] = measurement(0, calls[i].intValue(), false, true, 0, false, true);
			shift = rand.nextInt(calls[i].intValue());
			timereads[i] = measurement(shift, calls[i].intValue()+shift, false, false, 0, false, true);
		}

		print("Lesen Random Random", timereadr, calls, calls);
		print("Lesen Sortiert Random", timereads, calls, calls);
	}

	public void cud(){
		pWriter.println("compare random update-delete depending on databasesize, calls 1000000, percent 10");
		Double[] calls = new Double[]{0.0, 50000.0, 100000.0, 500000.0, 1000000.0, 2000000.0, 3000000.0, 4000000.0, 5000000.0};
		int len = calls.length;
		Double[] timefill = new Double[len];
		Double[] timereadr = new Double[len];
		Double[] timereadu = new Double[len];
		Double[] timereadd = new Double[len];
		Double[] callfill = new Double[len];
		Double[] callread = new Double[len];
		for(int i=1; i<len; i++){
			dbdrop();
			timefill[i] = measurement(0, calls[i].intValue(), true, true, 0, false, false);
			callfill[i] = calls[i];
			timereadr[i] = measurement(0, mil.intValue(), false, true, 0, false, false);
			timereadu[i] = measurement(0, mil.intValue(), false, true, 10, false, false);
			timereadd[i] = measurement(0, mil.intValue(), false, true, 10, true, false);
			callread[i] = mil;
		}

		print("Einfugen", timefill, callfill, calls);
		print("Lesen", timereadr, callread, calls);
		print("Update", timereadu, callread, calls);
		print("Delete", timereadd, callread, calls);
	}

	public void cudp(){
		pWriter.println("compare random update-delete depending on percent, databasesize 1000000, calls 1000000");
		Double[] calls = new Double[]{0.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
		int len = calls.length;
		Double[] timereadr = new Double[len];
		Double[] timereadu = new Double[len];
		Double[] timereadd = new Double[len];
		Double[] callread = new Double[len];
		for(int i=1; i<len; i++){
			dbdrop();
			measurement(0, mil.intValue(), true, true, 0, false, false);
			timereadr[i] = measurement(0, mil.intValue(), false, true, 0, false, false);
			timereadu[i] = measurement(0, mil.intValue(), false, true, calls[i].intValue(), false, false);
			timereadd[i] = measurement(0, mil.intValue(), false, true, calls[i].intValue(), true, false);
			callread[i] = mil;
		}

		print("Lesen", timereadr, callread, calls);
		print("Update", timereadu, callread, calls);
		print("Delete", timereadd, callread, calls);
	}

	public void call(){
		dbinit();
		try {
			pWriter = new PrintWriter(new FileWriter("measurement.txt", true), true);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		pWriter.println(dbname());
		crs(); // compares random and sorted fill and read dependent of the databasesize and checks if the readed key is correct
		crsn(); // compares random and sorted read dependent of the number of reads and checks if the readed key is correct
		cud(); // compares random delete and update dependent of the databasesize
		cudp(); // compares random delete and update dependent of the percent with read of the complete database
		dbdrop();
		dbclose();
	}

	public static void main(String[] args) {
		redisM a = new redisM();
		a.call();
		arangoM b = new arangoM();
		b.call();
		mongoM d = new mongoM();
		d.call();
		cassandraM c = new cassandraM();
		c.call();
	}
}