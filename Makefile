all:
	rustc elasticsearch.rc

test:
	rust --test elasticsearch.rc

deps:
	cargo install zmq
	cargo install uri

clean:
	rm -rf elasticsearch *.so *.dylib *.dSYM
