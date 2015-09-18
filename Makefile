
.PHONY: all clean build configure distclean doc apidoc

all: build

configure: distclean
	./configure --enable-sqlite --enable-postgresql --enable-test /tmp/usr/

build:
	ocaml setup.ml -build && \
	    rm -f main.byte main.native  && \
	    mv _build/src/test/main.native trakeva_tests

apidoc:
	mkdir -p _apidoc && \
	ocamlfind ocamldoc -html -d _apidoc/ \
            -package nonstd,pvem_lwt_unix,sqlite3,postgresql,sosa  \
	    -thread  -charset UTF-8 -t "Trakeva API" -keep-code -colorize-code \
	    -sort \
	    -I _build/src/lib/ \
	    -I _build/src/lib_sqlite/ \
	    -I _build/src/lib_postgresql/ \
	    -I _build/gen/lib_of_uri/ \
	    src/*/*.mli src/*/*.ml gen/*/*.mli

doc: apidoc build
	INPUT=src/doc/ \
	    INDEX=README.md \
	    TITLE_PREFIX="Trakeva: " \
	    OUTPUT_DIR=_doc \
	    API=_apidoc \
	    CATCH_MODULE_PATHS='^(Trakeva[A-Z_a-z]+):', \
	    TITLE_SUBSTITUTIONS="main.ml:Literate Tests" \
	    oredoc

clean:
	rm -fr _build trakeva_tests

distclean: clean
	ocaml setup.ml -distclean || echo OK ; \
	    rm -fr gen/ ; \
	    rm -f setup.ml _tags myocamlbuild.ml src/*/META src/*/*.mldylib src/*/*.mllib _oasis
