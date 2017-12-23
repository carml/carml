package com.taxonic.carml.rdf_mapper.util;

import static java.util.Objects.requireNonNull;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;

import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.impl.CarmlMapper;
import com.taxonic.carml.rdf_mapper.impl.MappingCache;

public class RdfObjectLoader <T> {

	private Function<Model, Set<Resource>> resourceSelector;
	private Class<T> clazz;
	private Model model;
	private UnaryOperator<Model> modelAdapter;
	private Consumer<MappingCache> populateCache;
	private Consumer<Mapper> configureMapper;

	private static final String
			RESOURCE_SELECTOR_MSG = "A resource selector must be provided",
			CLASS_MSG = "A class must be provided",
			MODEL_MSG = "A model must be provided",
			MODEL_ADAPTER_MSG = "A model adapter must be provided",
			REPOSITORY_MSG = "A repository must be provided",
			SPARQL_QUERY_MSG = "A SPARQL query must be provided",
			POPULATE_CACHE_MSG = "A cache populator must be provided";

	private RdfObjectLoader(
			Function<Model, Set<Resource>> resourceSelector,
			Class<T> clazz,
			Model model,
			UnaryOperator<Model> modelAdapter,
			Consumer<MappingCache> populateCache,
			Consumer<Mapper> configureMapper
	) {

		this.resourceSelector = requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
		this.clazz = requireNonNull(clazz, CLASS_MSG);
		this.model = requireNonNull(model, MODEL_MSG);
		this.modelAdapter = requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
		this.populateCache = requireNonNull(populateCache, POPULATE_CACHE_MSG);
		this.configureMapper = configureMapper;

	}

	public Set<T> load() {

		CarmlMapper mapper = new CarmlMapper();
		populateCache.accept(mapper);
		configureMapper.accept(mapper);

		Set<Resource> resources = resourceSelector.apply(model);

		return resources
				.stream()
				.<T>map(r -> mapper.map(modelAdapter.apply(model), r, clazz))
				.collect(ImmutableCollectors.toImmutableSet());
	}

	public static <T> BuilderOptionModelAdaptorOrModel newBuilder(Function<Model, Set<Resource>> resourceSelector,
											  Class<T> clazz) {
		return new BuilderModel(resourceSelector, clazz);
	}

	public static class BuilderModel<T> implements BuilderOptionModel,
			                                       BuilderOptionModelAdaptorOrModel,
			                                       BuilderOptionBuild,
			                                       BuilderOptionPopulateCacheOrBuild,
											       BuilderOptionConfigureMapper {

		private Function<Model, Set<Resource>> resourceSelector;
		private Class<T> clazz;
		private Model model;
		private UnaryOperator<Model> modelAdapter = m -> m;
		private Consumer<MappingCache> populateCache = c -> {};
		private Consumer<Mapper> configureMapper = m -> {};

		public BuilderModel(Function<Model, Set<Resource>> resourceSelector,
							Class<T> clazz) {
			this.resourceSelector = requireNonNull(resourceSelector, RESOURCE_SELECTOR_MSG);
			this.clazz = requireNonNull(clazz, CLASS_MSG);
		}

        @Override
		public BuilderOptionPopulateCacheOrBuild model(Model model) {
			this.model = requireNonNull(model, MODEL_MSG);
			return this;
		}

        @Override
		public BuilderOptionBuild model(Repository repository, Resource... contexts) {
			requireNonNull(repository, REPOSITORY_MSG);
			this.model = requireNonNull(QueryUtils.getModelFromRepo(repository, contexts), MODEL_MSG);
			return this;
		}

		@Override
		public BuilderOptionBuild model(Repository repository, String sparqlQuery) {
			requireNonNull(repository, REPOSITORY_MSG);
			requireNonNull(sparqlQuery, SPARQL_QUERY_MSG);
			this.model = requireNonNull(QueryUtils.getModelFromRepo(repository, sparqlQuery), MODEL_MSG);
			return this;
		}

		@Override
		public BuilderOptionModel modelAdapter(UnaryOperator<Model> modelAdapter) {
			this.modelAdapter = requireNonNull(modelAdapter, MODEL_ADAPTER_MSG);
			return this;
		}

		@Override
        public BuilderOptionConfigureMapper populateCache(Consumer<MappingCache> populateCache) {
			this.populateCache = requireNonNull(populateCache, POPULATE_CACHE_MSG);
			return this;
		}

		@Override
        public BuilderOptionBuild configureMapper(Consumer<Mapper> configureMapper) {
			this.configureMapper  = configureMapper;
			return this;
		}

		public RdfObjectLoader build() {
			return new RdfObjectLoader(resourceSelector,
					clazz,
					model,
					modelAdapter,
					populateCache,
					configureMapper);
		}
	}

	public interface BuilderOptionModelAdaptorOrModel extends BuilderOptionModel {
		BuilderOptionModel modelAdapter(UnaryOperator<Model> modelAdaptor);
	}

	public interface BuilderOptionModel {
		BuilderOptionPopulateCacheOrBuild model(Model model);

		BuilderOptionBuild model(Repository repository, Resource... contexts);

		BuilderOptionBuild model(Repository repository, String sparqlQuery);
	}

	public interface BuilderOptionBuild {
		RdfObjectLoader build();
	}

	public interface BuilderOptionPopulateCacheOrBuild extends BuilderOptionBuild {
        BuilderOptionConfigureMapper populateCache(Consumer<MappingCache> populateCache);
    }

    public interface BuilderOptionConfigureMapper {
		BuilderOptionBuild configureMapper(Consumer<Mapper> configureMapper);
	}
}
