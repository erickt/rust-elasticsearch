all:
	rustpkg install elasticsearch

test: all
	rustpkg install example

clean:
	rm -rf bin build lib
