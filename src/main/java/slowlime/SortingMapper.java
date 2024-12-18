package slowlime;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortingMapper extends Mapper<Object, Text, OutputKey, IntermediateRecord> {
    private static class InputRecord {
        public String category;
        public double revenue;
        public int qty;

        public InputRecord(String category, double revenue, int qty) {
            this.category = category;
            this.revenue = revenue;
            this.qty = qty;
        }

        public static InputRecord fromFields(String[] fields) {
            var category = fields[0];
            var revenue = Double.parseDouble(fields[1]);
            var qty = Integer.parseInt(fields[2]);

            return new InputRecord(category, revenue, qty);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        var fields = value.toString().split("\t");
        var record = InputRecord.fromFields(fields);
        var outKey = new OutputKey(new Text(record.category), record.revenue);
        var outValue = new IntermediateRecord(record.revenue, record.qty);
        context.write(outKey, outValue);
    }
}
