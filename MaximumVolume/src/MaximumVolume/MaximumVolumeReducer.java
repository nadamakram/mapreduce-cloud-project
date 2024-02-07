// Nada Handak 20481045

package MaximumVolume;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaximumVolumeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int maxVolume = Integer.MIN_VALUE;

        for (IntWritable value : values) {
            maxVolume = Math.max(maxVolume, value.get());
        }

        context.write(key, new IntWritable(maxVolume));
    }
}
