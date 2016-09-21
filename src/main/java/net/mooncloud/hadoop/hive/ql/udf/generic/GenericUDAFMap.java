package net.mooncloud.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * GenericUDAFMap.
 * 
 * @author yangjd
 *
 */
@Description(name = "collect_map", value = "_FUNC_(x, y) - Returns a map of objects with duplicates")
public class GenericUDAFMap extends AbstractGenericUDAFResolver {

	public GenericUDAFMap() {
	}

	@Override
	public GenericUDAFMapEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {

		if (parameters.length != 2) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly two arguments are expected.");
		}

		// if (parameters[0].getCategory() !=
		// ObjectInspector.Category.PRIMITIVE) {
		// throw new UDFArgumentTypeException(0,
		// "Only primitive type arguments are accepted but "
		// + parameters[0].getTypeName() + " is passed.");
		// }
		//
		// if (parameters[1].getCategory() !=
		// ObjectInspector.Category.PRIMITIVE) {
		// throw new UDFArgumentTypeException(1,
		// "Only primitive type arguments are accepted but "
		// + parameters[1].getTypeName() + " is passed.");
		// }

		// switch (parameters[0].getCategory()) {
		// case PRIMITIVE:
		// switch (parameters[1].getCategory()) {
		// case PRIMITIVE:
		// return new GenericUDAFMapEvaluator();
		// default:
		// throw new UDFArgumentTypeException(0,
		// "Only primitive type arguments are accepted but "
		// + parameters[1].getTypeName()
		// + " was passed as parameter 1.");
		// }
		// default:
		// throw new UDFArgumentTypeException(0,
		// "Only primitive type arguments are accepted but "
		// + parameters[0].getTypeName()
		// + " was passed as parameter 1.");
		// }

		switch (parameters[0].getCategory()) {
		case PRIMITIVE:
		case STRUCT:
		case MAP:
		case LIST:
			switch (parameters[1].getCategory()) {
			case PRIMITIVE:
			case STRUCT:
			case MAP:
			case LIST:
				return new GenericUDAFMapEvaluator();
			default:
				throw new UDFArgumentTypeException(0,
						"Only primitive, struct, list or map type arguments are accepted but "
								+ parameters[1].getTypeName()
								+ " was passed as parameter 2.");
			}
		default:
			throw new UDFArgumentTypeException(0,
					"Only primitive, struct, list or map type arguments are accepted but "
							+ parameters[0].getTypeName()
							+ " was passed as parameter 1.");
		}
	}
}