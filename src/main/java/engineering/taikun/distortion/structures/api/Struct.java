package engineering.taikun.distortion.structures.api;

public interface Struct {

	void set(int index, Object value);

	Object get(int index);

	int size();

}
