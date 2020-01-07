package store;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum StoreType {
	NULL(0),
	ELASTIC_HTTP(1),
	ELASTIC_NATIVE(2),
	STARDOG(3),
	;

	// putting this in the referencing file was causing a stack overflow during compilation. grr
	public static String x = Arrays.stream(StoreType.values())
	                               .map(enumVal -> String.format("%s(%d)", enumVal.name(), enumVal.ordinal()))
	                               .collect(Collectors.joining(", "));

	private int type;

	StoreType(int type) {
		this.type = type;
	}

	public int ofType() {
		return type;
	}
}
