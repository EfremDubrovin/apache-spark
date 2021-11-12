package eu.deltasource;

public class NumberWithSquareRoot {
	private final Integer number;
	private final Double squareRoot;

	public NumberWithSquareRoot(Integer number) {
		this.number = number;
		squareRoot = Math.sqrt(number);
	}
}
