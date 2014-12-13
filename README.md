Trakeva
=======

Transactions, Keys, and Values: an attempt at a generic interface for
transactional key-value stores with different backends.


The interfaces are defined in `Trakeva_interface`, we have, for now, two
implementations of the module type `Trakeva_interface.KEY_VALUE_STORE`:

- `Trakeva_git_commands` was extracted from
[Ketrew](http://seb.mondet.org/software/ketrew/index.html), it uses the `git`
executable to provide the functionality.
- `Trakeva_sqlite` (in the separate library `trakeva_sqlite`) uses 
[Sqlite3-ocaml](http://mmottl.github.io/sqlite3-ocaml/).

Build
-----

To install the libraries use opam:

    opam remote add smondet git@github.com:smondet/dev-opam-repo
    opam update
    opam install trakeva
    
Only if the library `sqlite3` is installed the library `trakeva_sqlite` will be
picked-up for installing too.

As a developer, just run:

    make configure
    make

(this will enable all backends and the tests).  The tests/benchmarks also depend
on [Sosa](http://seb.mondet.org/software/sosa/index.html) (`opam install sosa`).

To run the unit tests:

    ./trakeva_tests

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
    export PKG_CONFIG_PATH=/usr/local//Cellar/sqlite/3.8.0.2/lib/pkgconfig
    opam install sqlite3

