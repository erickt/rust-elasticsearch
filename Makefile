all:
	rustc elasticsearch.rs

test:
	rustc --test elasticsearch.rs

example: all
	rustc -L . example.rs

deps:
	cargo install zmq
	cargo install url

clean:
	rm -rf elasticsearch example *.so *.dylib *.dSYM
