package store;

import java.util.List;

import core.WorkerTaskResult;

public interface Store {

	public void initStore();

	/**
	 * TODO : why declare a return type if it's never used?
	 */
	public boolean indexData(long id, String dbName, String path, String sourceName, String columnName,
	                         List<String> values);

	public boolean storeDocument(WorkerTaskResult wtr);

	public void tearDownStore();
}
