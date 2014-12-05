
.PHONY: all clean build configure distclean doc apidoc

all: build

configure: distclean
	oasis setup -setup-update dynamic && \
	    ocaml setup.ml -configure && \
	    echo 'Configured'

build:
	ocaml setup.ml -build && \
	    rm -f main.byte main.native  && \
	    mv _build/src/test/main.native trakeva_tests

apidoc:
	mkdir -p _apidoc && \
	ocamlfind ocamldoc -html -d _apidoc/ -package ketrew  \
	    -thread  -charset UTF-8 -t "Trakeva API" -keep-code -colorize-code \
	    -sort \
	    -I _build/src/lib/ \
	    src/*/*.mli src/*/*.ml

doc: apidoc build
	INPUT=src/doc/ \
	    INDEX=README.md \
	    TITLE_PREFIX="Ksinai: " \
	    OUTPUT_DIR=_doc \
	    COMMAND_SUBSTITUTIONS=vcshootout:./vcshootout,kserver:./kserver  \
	    API=_apidoc \
	    CATCH_MODULE_PATHS='^(Trakeva[A-Z_a-z]+):', \
	    TITLE_SUBSTITUTIONS="main.ml:Literate Tests" \
	    oredoc

clean:
	rm -fr _build trakeva_tests

distclean: clean
	ocaml setup.ml -distclean || echo OK ; \
	    rm -f setup.ml _tags myocamlbuild.ml src/*/META src/*/*.mldylib src/*/*.mllib
