package com.taxonic.carml.engine.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
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
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
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
		
		Function<Value, Object> adapter;
		
		if (type.equals(Integer.TYPE) || type.equals(Integer.class)) {
			adapter = v -> {
				if (v == null) {
					// Return null for empty function parameter
					return null;
				}
				
				if (!(v instanceof Literal))
					throw new IllegalArgumentException(
						"value [" + v + "] was not a literal, which is expected " +
						"for a parameter of type int or Integer"
					);
				Literal l = (Literal) v;
				return l.intValue();
			};
		}
		
		else if (type.equals(String.class)) {
			adapter = v -> {
				if (v == null) {
					// Return null for empty function parameter
					return null;
				}
				
				if (!(v instanceof Literal))
					throw new IllegalArgumentException(
						"value [" + v + "] was not a literal, which is expected " +
						"for a parameter of type String"
					);
				Literal l = (Literal) v;
				return l.stringValue();
			};
		}
		
		// TODO more parameter types
		
		else
			throw new RuntimeException("parameter type [" + type + "] not (yet) supported");

		return new ExtractParameter() {
			
			@Override
			public Object extract(Model model, Resource subject) {
				Optional<Value> object = Models.object(model.filter(subject, iri, null));
				Value value = object.orElse(null);
				return adapter.apply(value);
			}
		};
	}
	
	public int size() {
		return functions.size();
	}
		
}
