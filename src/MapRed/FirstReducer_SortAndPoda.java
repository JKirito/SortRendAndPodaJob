package MapRed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import entities.CeldaWritable;
import entities.CoordenadaConRendWritable;
import entities.CoordenadaWritable;

public class FirstReducer_SortAndPoda extends
		Reducer<IntWritable, CoordenadaConRendWritable, IntWritable, DoubleWritable> {

	private final Double anchoCelda = 0.0001;

	public void reduce(IntWritable _key, Iterable<CoordenadaConRendWritable> values, Context context)
			throws IOException, InterruptedException {

		Double total = 0.0;
		int i=0;
		List<CoordenadaConRendWritable> coorRends = new ArrayList<CoordenadaConRendWritable>();
		for (CoordenadaConRendWritable C : values) {
			CoordenadaConRendWritable D = new CoordenadaConRendWritable();
			CoordenadaWritable coo = new CoordenadaWritable();
			coo.setLatitud(C.getCoordenada().getLatitud());
			coo.setLongitud(C.getCoordenada().getLongitud());
			D.setCoordenada(coo);
			D.setRend(C.getRend());
			coorRends.add(i, D);
			i++;
			if (total > 0)
				break;
			total += C.getRend().get();
		}
		System.out.println("PRIMER TOTAL: " + total);
		total = 0.0;
		// Collections.sort(coorRends);
		for (CoordenadaConRendWritable C : coorRends) {
			System.out.println(C.getCoordenada());
			total += C.getRend().get();
		}
		System.out.println(coorRends.size());
		System.out.println("total2: " + total);

		// =25437*0,15	MIN	3816 (15 pencentil)
		// =21622*0,9	MAX	19460 (90 percentil)

//		 OperadorRend oper = new OperadorRend(values);

		// BuscadorCasilla b = new BuscadorCasilla(C, anchoCelda);
		CeldaWritable celda = new CeldaWritable(null, null);
		DoubleWritable rend = new DoubleWritable(32.123);
		// context.write(_key, new DoubleWritable(oper.sumaSinOutliers(10,
		// 95)));
		context.write(_key, rend);
	}
}
