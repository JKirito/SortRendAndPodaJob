package MapRed;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import entities.CoordenadaConRendWritable;
import entities.CoordenadaWritable;
import entities.Monitor.MonitorCampo;

public class FirstMapper_GroupCoordRend extends Mapper<LongWritable, Text, IntWritable, CoordenadaConRendWritable> {

	private static final String SEPARATOR_SYMBOL = ",";
	public String latitudCampo;
	public String longitudCampo;
	public final IntWritable key = new IntWritable(1);

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		final String[] values = ivalue.toString().split(SEPARATOR_SYMBOL);

		latitudCampo = values[MonitorCampo.LATITUD.value()];
		longitudCampo = values[MonitorCampo.LONGITUD.value()];

		if (latitudCampo.equals(MonitorCampo.LATITUD.name()) || longitudCampo.equals(MonitorCampo.LONGITUD.name())) {
			return;
		}

		// Solo me quedo con los de un monitor
		if (!values[MonitorCampo.FNAME.value()].toString().equals("0JpiX3keB7")) {
			System.out.println("saltenado linea xq es del monitor... " + values[MonitorCampo.FNAME.value()].toString());
			return;
		}

		DoubleWritable latitud = new DoubleWritable(Double.parseDouble(latitudCampo));
		DoubleWritable longitud = new DoubleWritable(Double.parseDouble(longitudCampo));
		CoordenadaWritable coordenada = new CoordenadaWritable(longitud, latitud);
		DoubleWritable rend = new DoubleWritable(Double.parseDouble(values[MonitorCampo.REND.value()]));
		CoordenadaConRendWritable coordenadaRend = new CoordenadaConRendWritable(coordenada, rend);

		context.write(key, coordenadaRend);
//		System.out.println(latitud + "   -   "+longitud+"   -   "+ rend);
	}

}
