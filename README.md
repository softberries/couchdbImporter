couchdbImporter
===============

Simple Erlang/Ruby script to import XML data into CouchDB

Execute Ruby script:

1. cat dbdump_artistalbumtrack.0.290905586176.xml | ruby ../path_path_to_jamendo_rb_file/jamendo.rb

Execute Erlang script:

1. change the path within the script
2. compile within the erlang shell:

c(xmlimporter).

3. run

xmlimporter:start().

