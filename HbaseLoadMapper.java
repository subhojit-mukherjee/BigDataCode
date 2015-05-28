/*public class HbaseLoadMapper {

}
*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HbaseLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private String hbaseTable;	
    private String dataSeperator;
    private String columnFamily1;
    private String columnFamily2;
    private ImmutableBytesWritable hbaseTableName;

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();		
        hbaseTable = configuration.get("hbase.table.name");		
        dataSeperator = configuration.get("data.seperator");		
        columnFamily1 = configuration.get("COLUMN_FAMILY_1");		
        columnFamily2 = configuration.get("COLUMN_FAMILY_2");		
        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));		
    }

    public void map(LongWritable key, Text value, Context context) {
        try {		
            String[] values = value.toString().split(dataSeperator);			
            String rowKey = values[0];		
            String movieNameYear=values[1];
            String name=movieNameYear.substring(0, movieNameYear.length()-6);
            String year=movieNameYear.substring(movieNameYear.length()-5, movieNameYear.length()-1);
            Put put = new Put(Bytes.toBytes(rowKey));			
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes(name));			
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("year"), Bytes.toBytes(year));			
            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("genre"), Bytes.toBytes(values[2]));					
            context.write(hbaseTableName, put);			
        } catch(Exception exception) {			
            exception.printStackTrace();			
        }
    }
}