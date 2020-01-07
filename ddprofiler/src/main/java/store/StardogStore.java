package store;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.complexible.stardog.api.Adder;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.stardog.stark.IRI;
import com.stardog.stark.Values;

import com.google.common.base.Joiner;
import core.WorkerTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Store} backend to Stardog
 */
class StardogStore implements Store {
	private static final Logger LOGGER = LoggerFactory.getLogger(StardogStore.class);

	private static final IRI TEXT_graph = Values.iri("aurum:text:graph");

	private static final IRI PROFILE_graph = Values.iri("aurum:profile:graph");

	private Connection mConn;

	@Override
	public void initStore() {
		mConn = ConnectionConfiguration.at("https://localhost:443/stardog/aurum01");
		mConn.begin();
		mConn.remove().context(TEXT_graph).context(PROFILE_graph);
		mConn.commit();
	}

	/**
	 * Called directly by {@link core.FilterAndBatchDataIndexer#indexData(String, String, Map)}
	 * - only for string-typed fields (dataType=T)
	 */
	@Override
	public boolean indexData(long id, String dbName, String path, String sourceName, String columnName, List<String> values) {
//		long attrId = Utils.computeAttrId(dbName, sourceName, columnName);
		// TODO : this needs URL escaping?
		String localName = Joiner.on(":").join(id, dbName, path, sourceName, columnName);
		System.err.println("Indexing for: " + localName);
		IRI columnIri = Values.iri("column:", localName);
		try {
			mConn.begin();
			Adder adder = mConn.add();
			adder.statement(columnIri, Values.iri("prop:id"), Values.literal(id), TEXT_graph);
			adder.statement(columnIri, Values.iri("prop:dbName"), Values.literal(dbName), TEXT_graph);
			adder.statement(columnIri, Values.iri("prop:path"), Values.literal(path), TEXT_graph);
			adder.statement(columnIri, Values.iri("prop:sourceName"), Values.literal(sourceName), TEXT_graph);
			adder.statement(columnIri, Values.iri("prop:columnName"), Values.literal(columnName), TEXT_graph);
			values.forEach(v -> {
				// TODO : N.B.! RDF representation here is -set- semantics, unlike the array version used in ES
				//        (could be equal in the eventual TF-IDF computation in `build_schema_sim_relation()`
				adder.statement(columnIri, Values.iri("prop:value"), Values.literal(v), TEXT_graph);
			});
			mConn.commit();
		}
		catch (RuntimeException ex) {
			mConn.rollback();
			throw ex;
		}
		return false;
	}

	@Override
	public boolean storeDocument(WorkerTaskResult wtr) {
		String localName = Joiner.on(":").join(wtr.getId(), wtr.getDBName(), wtr.getPath(), wtr.getSourceName(), wtr.getColumnName());
		System.err.println("Profile for: " + localName);
		IRI columnIri = Values.iri("column:", localName);
		try {
			mConn.begin();
			Adder adder = mConn.add();
			// TODO : this is only "N" (numeric) or "T" (text)? We should get int/float distinction if possible?
			adder.statement(columnIri, Values.iri("prop:getDataType"), Values.literal(wtr.getDataType()), PROFILE_graph);
			adder.statement(columnIri, Values.iri("prop:getTotalValues"), Values.literal(wtr.getTotalValues()), PROFILE_graph);
			adder.statement(columnIri, Values.iri("prop:getUniqueValues"), Values.literal(wtr.getUniqueValues()), PROFILE_graph);
			if ("N".equals(wtr.getDataType())) {
				adder.statement(columnIri, Values.iri("prop:getMinValue"), Values.literal(wtr.getMinValue()), PROFILE_graph);
				adder.statement(columnIri, Values.iri("prop:getMaxValue"), Values.literal(wtr.getMaxValue()), PROFILE_graph);
				adder.statement(columnIri, Values.iri("prop:getAvgValue"), Values.literal(wtr.getAvgValue()), PROFILE_graph);
				adder.statement(columnIri, Values.iri("prop:getMedian"), Values.literal(wtr.getMedian()), PROFILE_graph);
				adder.statement(columnIri, Values.iri("prop:getIQR"), Values.literal(wtr.getIQR()), PROFILE_graph);
			}
			else if ("T".equals(wtr.getDataType())) {
				if (!wtr.getEntities().isEmpty()) {
					adder.statement(columnIri, Values.iri("prop:getEntities"), Values.literal(wtr.getEntities()), PROFILE_graph);
				}
				adder.statement(columnIri, Values.iri("prop:minHash"), Values.literal(Arrays.toString(wtr.getMH())), TEXT_graph);
			}
			else {
				LOGGER.warn("Unknown type " + wtr.getDataType() + " for " + localName);
			}
			mConn.commit();
		}
		catch (RuntimeException ex) {
			mConn.rollback();
			throw ex;
		}
		return false;
	}

	@Override
	public void tearDownStore() {
		mConn.close();
	}
}
