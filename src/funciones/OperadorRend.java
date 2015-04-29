package funciones;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;

public class OperadorRend {

	/**
	 * suma de todos los valores de rend
	 */
	private Double total;
	private Double promedio;
	private List<Double> sortRendValues;
	private List<Double> valuesWithoutOutliers;

	public OperadorRend(Iterable<DoubleWritable> values) {
		this.sortRendValues = new ArrayList<Double>();
		this.total = 0.0;
		for (DoubleWritable val : values) {
			this.sortRendValues.add(val.get());
			total += val.get();
		}
		// TODO: si uso secundary sort, es necesario ordenar la lista?
		// TODO: ordenar por lote en vez de celda???
		Collections.sort(this.sortRendValues);
	}

	public Double getPromedio() {
		if (this.promedio == null) {
			this.promedio = (this.total / this.sortRendValues.size());
		}
		return this.promedio;
	}

	/**
	 * Devuelve una nueva lista, más adelante eliminar los outliers sin crear
	 * una nueva lista para ahorrar RAM. Para eliminar los percentiles menor y
	 * mayor, creo primero dos listas, una con los valores por debajo del
	 * promedio, y otra con los valores por encima del promedio
	 *
	 * @param percentilMenor
	 * @param percentilMayor
	 * @return una NUEVA lista con los percentiles de los parametros.
	 */
	// public List<Double> getListWithoutOutliers(double percentilMenor, double
	// percentilMayor) {
	// if (this.valuesWithoutOutliers == null) {
	// List<Double> maxValues = new ArrayList<Double>();
	// List<Double> minValues = new ArrayList<Double>();
	//
	// for (Double value : this.sortRendValues) {
	// if (value.compareTo(getPromedio()) > 0) {
	// maxValues.add(value);
	// } else {
	// minValues.add(value);
	// }
	// }
	// int cantMenorAEliminar = cantEliminar(minValues.size(), percentilMenor);
	// int cantMayorAEliminar = cantEliminar(maxValues.size(), percentilMayor);
	//
	// for (Double value : this.sortRendValues) {
	// if (minValues.contains(value)) {
	// for (int i = cantMenorAEliminar; i < minValues.size(); i++) {
	// this.valuesWithoutOutliers.add(minValues.get(i));
	// }
	// } else if (maxValues.contains(value)) {
	// for (int i = 0; i < maxValues.size() - cantMayorAEliminar; i++) {
	// this.valuesWithoutOutliers.add(maxValues.get(i));
	// }
	// }
	// }
	// }
	//
	// return this.valuesWithoutOutliers;
	// }

	/**
	 * Devuelve una nueva lista, más adelante eliminar los outliers sin crear
	 * una nueva lista para ahorrar RAM
	 *
	 * @param percentilMenor
	 * @param percentilMayor
	 * @return una NUEVA lista con los percentiles de los parametros.
	 */
	public List<Double> getListWithoutOutliers(double percentilMenor, double percentilMayor) {
		if (this.valuesWithoutOutliers == null) {
			this.valuesWithoutOutliers = new ArrayList<Double>();

			// resto 1 porque las listas comienzan en la pos cero.
			int posPercentilMenor = cantEliminar(this.sortRendValues.size(), percentilMenor) - 1;

			// Quito a la lista original el percentil menor
			this.valuesWithoutOutliers.addAll(this.sortRendValues.subList(posPercentilMenor, sortRendValues.size()));

			// Quito el percentil mayor a la lista a la cual ya quite el percentil menor
			int posPercentilMayor = cantEliminar(this.valuesWithoutOutliers.size(), percentilMayor) - 1;
			this.valuesWithoutOutliers = this.valuesWithoutOutliers.subList(0, posPercentilMayor+1);
		}

		return this.valuesWithoutOutliers;
	}

	/**
	 *
	 * @param size
	 *            : cantidad de valores
	 * @param percentil
	 *            : valor que se eliminará (ES %: 10% debe ser 10)
	 * @return
	 */
	private int cantEliminar(int size, double percentilPorcentaje) {
		Double percentil = new Double((size * percentilPorcentaje) / 100);
		System.out.println("percentil " + percentilPorcentaje + " de " + size + " valores es: " + percentil
				+ " parse a: " + new Double(Math.ceil(percentil)).intValue());
		return new Double(Math.ceil(percentil)).intValue();
	}

	public Double sumaSinOutliers(double percentilMenor, double percentilMayor) {
		if (this.valuesWithoutOutliers == null) {
			getListWithoutOutliers(percentilMenor, percentilMayor);
		}
		Double suma = 0.0;
		for (Double val : this.valuesWithoutOutliers) {
			suma += val;
		}
		return suma;
	}

}
