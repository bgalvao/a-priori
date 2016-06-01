package baseCreator;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Outputs [ITEMNAME, line number]. */
public class Mapper1 extends Mapper<Object, Text, Text, Text> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String items[] = value.toString().split(",");
		for (String i : items) {
			context.write(new Text(i), new Text(key.toString()));
		}
	}
}
