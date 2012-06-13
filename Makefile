all:
	rustc elasticsearch.rc

test:
	rustc --test elasticsearch.rc

example: all
	rustc -L . example.rs

deps:
	cargo install zmq
	cargo install uri

clean:
	rm -rf elasticsearch *.so *.dylib *.dSYM
