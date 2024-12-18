package slowlime;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupingReducer extends Reducer<Text, IntermediateRecord, Text, IntermediateRecord> {
    @Override
    protected void reduce(Text k, Iterable<IntermediateRecord> vs, Context ctx)
            throws IOException, InterruptedException {
        var result = new IntermediateRecord();

        for (var v : vs) {
            result.price += v.price;
            result.qty += v.qty;
        }

        ctx.write(k, result);
    }
}
