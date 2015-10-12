Trakeva
=======

Transactions, Keys, and Values: an attempt at a generic interface for
transactional key-value stores with different backends.


The interfaces are defined in the [Trakeva](src/lib/trakeva.ml) module.
We have, for now, one DB implementation of the module type `Trakeva.KEY_VALUE_STORE`:

- `Trakeva_sqlite` (in the separate library `trakeva_sqlite`) uses
  [Sqlite3-ocaml](http://mmottl.github.io/sqlite3-ocaml/).
- `Trakeva_postgresql` (in the separate library `trakeva_postgresql`) uses
  [`postgresql-ocaml`](http://mmottl.github.io/postgresql-ocaml/)
- `Trakeva_of_uri` (in the separate library `trakeva_of_uri`)
  chooses dynamically between the 2 backends above thanks to the URI passed to
  the `Trakeva_of_uri.load` function. The backends are optionally compiled so
  they might be missing;`load` would then throw an exception.

There is also a very basic in “in-memory” cache functor, adding a cache layer on
top of any key-value DB, cf. `Trakeva_cache`.

This is Trakeva `0.0.0+master`, see also the repository
[`smondet/trakeva`](https://github.com/smondet/trakeva) for issues/questions.

Build
-----

To install the libraries use opam:

    opam install trakeva

If the packages `sqlite3` and/or `postgresql` are installed, then the libraries
`trakeva_sqlite` and/or `trakeva_postgresql` will be picked-up for installation
too.


To build the development version from the repository, just run:

    make configure
    make

(this will enable all backends and the tests).

Tests
-----

The tests/benchmarks also depend on the
[Sosa](http://seb.mondet.org/software/sosa/index.html) library
(`opam install sosa`).

To run the unit tests:

    ./trakeva_tests

The main test starts a temporary Postgresql server on port `4242`, this can be
overridden with the environment variable `POSTGRESQL_PORT`.
To use an existing database server instead of creating one use the variable
`POSTGRESQL_CONNECTION_INFO`.

To run the benchmarks:

    ./trakeva_tests bench collection=100 kb=5     # quick bench
    ./trakeva_tests bench collection=1000 kb=20   # about a minute long
 
To build the documentation:

    make doc

and checkout `_doc/index.html`.


Notes/Tips
----------

### Sqlite3 On MacOSX

See the issues [#50](https://github.com/ocaml/opam-repository/issues/50), on the
opam-repository and 
[#21129](https://github.com/Homebrew/homebrew/issues/21129) on Homebrew.  So
please use:

    brew install pkg-config sqlite
    export PKG_CONFIG_PATH=`find /usr/local/Cellar/sqlite -depth 1 | tail -n 1`/lib/pkgconfig
    opam install sqlite3

