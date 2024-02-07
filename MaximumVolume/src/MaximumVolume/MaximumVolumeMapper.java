// Nada Handak 20481045

package MaximumVolume;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaximumVolumeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private Text quote = new Text();
    private IntWritable volume = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (key.get() == 0 && value.toString().contains("Quote, Date, Open, High, Low, Close, Adj Close, Volume")){
            return;
        }

        try{
            String[] tokens = value.toString().split(",");
            String quoteValue = tokens[0].trim();
            int volumeValue = Integer.parseInt(tokens[7].trim());

            quote.set(quoteValue);
            volume.set(volumeValue);

            context.write(quote, volume);
        }

        catch (NumberFormatException e){
            System.err.println("Error " + e.getMessage());
        }


    }
}

