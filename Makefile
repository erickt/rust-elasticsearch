all:
	rustc elasticsearch.rc

test:
	rust --test elasticsearch.rc

deps:
	cargo install -g zmq
	cargo install -g uri

clean:
	rm -rf elasticsearch *.so *.dylib *.dSYM
