all:
	rustc elasticsearch.rc

test:
	rustc --test elasticsearch.rc

example: all
	rustc -L . example.rs

deps:
	cargo install zmq
	cargo install url

clean:
	rm -rf elasticsearch *.so *.dylib *.dSYM
