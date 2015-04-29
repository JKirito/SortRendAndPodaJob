package entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class CoordenadaConRendWritable implements WritableComparable<CoordenadaConRendWritable> {
	// TODO: Y si fuera sólo 'double', que ventajas/desventajas tendría??
	private CoordenadaWritable coordenada;
	private DoubleWritable rend;

	public CoordenadaConRendWritable() {
		this.coordenada = new CoordenadaWritable();
		this.rend = new DoubleWritable();
	}

	public CoordenadaConRendWritable(CoordenadaWritable coordenada, DoubleWritable rend) {
		this.coordenada = coordenada;
		this.rend = rend;
	}

	public CoordenadaWritable getCoordenada() {
		return coordenada;
	}

	public DoubleWritable getRend() {
		return rend;
	}

	public void setCoordenada(CoordenadaWritable coordenada) {
		this.coordenada = coordenada;
	}

	public void setRend(DoubleWritable rend) {
		this.rend = rend;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.coordenada.readFields(in);
		this.rend.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.coordenada.write(out);
		rend.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((coordenada == null) ? 0 : coordenada.hashCode());
		result = prime * result + ((rend == null) ? 0 : rend.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CoordenadaConRendWritable other = (CoordenadaConRendWritable) obj;
		if (coordenada == null) {
			if (other.coordenada != null)
				return false;
		} else if (!coordenada.equals(other.coordenada))
			return false;
		if (rend == null) {
			if (other.rend != null)
				return false;
		} else if (!rend.equals(other.rend))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return this.coordenada.toString() + "	" + rend;
	}

	// TODO! solo me sirve el rinde para esto???
	@Override
	public int compareTo(CoordenadaConRendWritable cw) {
		// int cmp = longitud.compareTo(cw.longitud);
		// if (cmp != 0) {
		// return cmp;
		// }
		// cmp = latitud.compareTo(cw.latitud);
		// if (cmp != 0) {
		// return cmp;
		// }
		return this.rend.compareTo(cw.rend);
	}

	//ODO: ver!!!
//	public static class Comparator extends WritableComparator {
//
//		private static final DoubleWritable.Comparator DOUBLE_COMPARATOR = new DoubleWritable.Comparator();
//
//		public Comparator() {
//			super(CoordenadaConRendWritable.class);
//		}
//
//		@Override
//		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//			try {
//				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
//				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
//				return DOUBLE_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
//			} catch (IOException e) {
//				throw new IllegalArgumentException(e);
//			}
//		}
//
//		static {
//			WritableComparator.define(CoordenadaConRendWritable.class, new Comparator());
//		}
//	}
}
