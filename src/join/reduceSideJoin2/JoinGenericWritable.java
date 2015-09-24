package join.reduceSideJoin2;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

// When two sequence files, which have same Key type but different Value types, are mapped out to reduce, 
// multiple value types is not allowed. In this case, this class can help you wrap instances with different types.
public class JoinGenericWritable extends GenericWritable {

	private static Class<? extends Writable>[] CLASSES = null;

	// defines the classes which will be wrapped in GenericObject in
	// application.
	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] {
				SalesOrderDataRecord.class, ProductRecord.class };
	}

	public JoinGenericWritable() {
	}

	public JoinGenericWritable(Writable instance) {
		// Set the instance that is wrapped.
		set(instance);
	}

	// this classes defined in getTypes() method, must implement Writable
	// interface.
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}

}
