rust-elasticsearch is a [Rust language](http://rust-lang.org) binding for
the [Elasticsearch](http://elasticsearch.org) fulltext search engine.

Installation
------------

Install for the users of rust-elasticsearch:

    % cargo install zmq
    % cargo install uri
    % cargo install elasticseach

Install for developers:

    % git clone https://github.com/erickt/rust-elasticsearch
    % cd rust-elasticsearch
    % make deps
    % make

Setting up Elasticsearch
------------------------

rust-elasticsearch uses the
[transport-zeromq](https://github.com/tlrx/transport-zeromq) plugin to
connect with Elasticsearch. Unfortunately it can be a little tricky to set
up. Here is how I got it to work. First, install the [Java
Zeromq](https://github.com/zeromq/jzmq) bindings:

    % wget https://github.com/zeromq/jzmq/zipball/v1.0.0
    % unzip zeromq-jzmq-semver-0-gdaf4775
    % cd zeromq-jzmq-8522576
    % ./configure
    % make
    % make install

Next, download Elasticsearch:

    % wget https://github.com/downloads/elasticsearch/elasticsearch/elasticsearch-0.19.4.zip
    % unzip elasticsearch-0.19.4.zip

After that, install the
[transport-zeromq](https://github.com/tlrx/transport-zeromq) plugin:

    % cd elasticsearch-0.19.4
    % ./bin/plugin -install tlrx/transport-zeromq/0.0.3

Finally, start Elasticsearch. You may need to explicitly set the shared library path. On Linux, do:

    % cd $elasticsearch_dir
    % export LD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/usr/local/lib
    % ./bin/elasticsearch -f

And Macs, do:

    % cd $elasticsearch_dir
    % export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/usr/local/lib
    % ./bin/elasticsearch -f
