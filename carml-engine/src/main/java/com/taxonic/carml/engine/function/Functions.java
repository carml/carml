package com.taxonic.carml.engine.function;

import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Functions {

	private static final Logger LOG = LoggerFactory.getLogger(Functions.class);

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	private Map<IRI, ExecuteFunction> functions = new LinkedHashMap<>();

	public Optional<ExecuteFunction> getFunction(IRI iri) {
		return Optional.ofNullable(functions.get(iri));
	}

	public void addFunctions(Object fn) {
		Arrays.asList(fn.getClass().getMethods())
			.stream()
			.map(m -> createFunctionExecutor(fn, m))
			.filter(Optional::isPresent)
			.map(Optional::get)
			.forEach(f -> functions.put(f.getIri(), f));
	}

	private Optional<ExecuteFunction> createFunctionExecutor(Object obj, Method method) {

		FnoFunction function = method.getAnnotation(FnoFunction.class);
		if (function == null) return Optional.empty();
		IRI iri = VF.createIRI(function.value());

		List<ExtractParameter> parameterExtractors =
			Arrays.asList(method.getParameters())
				.stream()
				.map(this::createParameterExtractor)
				.collect(Collectors.toList());

		LOG.debug("Creating executable FnO function {}", function);
		return Optional.of(new ExecuteFunction() {

			@Override
			public Object execute(Model model, Resource subject) {

				List<Object> arguments = parameterExtractors
					.stream()
					.map(e -> e.extract(model, subject))
					.collect(Collectors.toList());

				try {
					// TODO return value adapter?
					LOG.trace("Executing function {} with arguments {}", method.getName(), arguments);
					return method.invoke(obj, arguments.toArray());
				}
				catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					throw new RuntimeException("error executing function", e);
				}
			}

			@Override
			public IRI getIri() {
				return iri;
			}
		});
	}

	private ExtractParameter createParameterExtractor(Parameter parameter) {

		FnoParam param = parameter.getAnnotation(FnoParam.class);
		if (param == null)
			throw new RuntimeException("no @" + FnoParam.class.getName() +
				" annotation present on parameter");
		IRI iri = VF.createIRI(param.value());

		Type type = parameter.getType();

		Function<List<Value>, Object> adapter;

		if (type.equals(Integer.TYPE) || type.equals(Integer.class)) {
			adapter = l -> {
				if (l == null || l.isEmpty()) {
					// Return null for empty function parameter
					return null;
				}

				expectSingleValue(l);
				return literalToInt(l.get(0));
			};
		}

		else if (type.equals(String.class)) {
			adapter = l -> {
				if (l == null || l.isEmpty()) {
					// Return null for empty function parameter
					return null;
				}

				expectSingleValue(l);
				return literalToString(l.get(0));
			};
		}

		else if (type.equals(Double.TYPE) || type.equals(Double.class)) {
			adapter = l -> {
				if (l == null || l.isEmpty()) {
					// Return null for empty function parameter
					return null;
				}

				expectSingleValue(l);
				return literalToDouble(l.get(0));
			};
		}

		else if (type.equals(Float.TYPE) || type.equals(Float.class)) {
			adapter = l -> {
				if (l == null || l.isEmpty()) {
					// Return null for empty function parameter
					return null;
				}

				expectSingleValue(l);
				return literalToFloat(l.get(0));
			};
		}

		else if (type.equals(Long.TYPE) || type.equals(Long.class)) {
			adapter = l -> {
				if (l == null || l.isEmpty()) {
					// Return null for empty function parameter
					return null;
				}

				expectSingleValue(l);
				return literalToLong(l.get(0));
			};
		}

		else if (type.equals(Boolean.TYPE) || type.equals(Boolean.class)) {
			adapter = l -> {
				if (l == null || l.isEmpty()) {
					// Return null for empty function parameter
					return null;
				}

				expectSingleValue(l);
				return literalToBoolean(l.get(0));
			};
		}

		else if (Collection.class.isAssignableFrom(parameter.getType())) {
			// TODO: Currently only collections with string parameter type supported.
			adapter = l -> {
				if (l == null || l.isEmpty()) {
					// Return null for empty function parameter
					return null;
				}
				if (!(l instanceof Collection)) {
					throw new IllegalArgumentException(
						"value [" + l + "] was not a collection, which is expected " +
						"for a parameter of type Collection<?>"
					);
				}

				return l.stream()
						.map(Value::stringValue)
						.collect(ImmutableCollectors.toImmutableList());
			};
		}

		else
			throw new RuntimeException("parameter type [" + type + "] not (yet) supported");

		return new ExtractParameter() {

			@Override
			public Object extract(Model model, Resource subject) {
				Model paramValues = model.filter(subject, iri, null);

				List<Value> values = paramValues.stream()
				.map(Statement::getObject)
				.collect(ImmutableCollectors.toImmutableList());

				return adapter.apply(values);
			}
		};
	}

	public int size() {
		return functions.size();
	}

	private void expectSingleValue(List<Value> values) {
		if (values.size() > 1) {
			throw new IllegalArgumentException(
				"value [" + values + "] has more than one value, which is not expected."
			);
		}
	}

	private String literalToString(Value v) {
		if (!(v instanceof Literal)) {
			throw new IllegalArgumentException(
				"value [" + v + "] was not a literal, which is expected " +
				"for a parameter of type String."
			);
		}
		Literal literal = (Literal) v;
		return literal.stringValue();
	}

	private int literalToInt(Value v) {
		if (!(v instanceof Literal)) {
			throw new IllegalArgumentException(
				"value [" + v + "] was not a literal, which is expected " +
				"for a parameter of type int or Integer."
			);
		}
		Literal literal = (Literal) v;
		return literal.intValue();
	}

	private double literalToDouble(Value v) {
		if (!(v instanceof Literal)) {
			throw new IllegalArgumentException(
				"value [" + v + "] was not a literal, which is expected " +
				"for a parameter of type double or Double."
			);
		}
		Literal literal = (Literal) v;

		return literal.doubleValue();
	}

	private float literalToFloat(Value v) {
		if (!(v instanceof Literal)) {
			throw new IllegalArgumentException(
				"value [" + v + "] was not a literal, which is expected " +
				"for a parameter of type float or Float."
			);
		}
		Literal literal = (Literal) v;

		return literal.floatValue();
	}

	private long literalToLong(Value v) {
		if (!(v instanceof Literal)) {
			throw new IllegalArgumentException(
				"value [" + v + "] was not a literal, which is expected " +
				"for a parameter of type long or Long."
			);
		}
		Literal literal = (Literal) v;

		return literal.longValue();
	}

	private boolean literalToBoolean(Value v) {
		if (!(v instanceof Literal)) {
			throw new IllegalArgumentException(
				"value [" + v + "] was not a literal, which is expected " +
				"for a parameter of type boolean or Boolean."
			);
		}
		Literal literal = (Literal) v;

		return literal.booleanValue();
	}

}
