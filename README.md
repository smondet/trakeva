Trakeva
=======

Transactions, Keys, and Values: an attempt at a generic interface for
transactional key-value stores with different backends.

For now, this is code extracted from
[Ketrew](http://seb.mondet.org/software/ketrew/index.html).

Build
-----

As a developer, just run:

    make configure
    make

To install with `opam`:

    oasis2opam --local
    opam pin add trakeva .

To build the documentation:

    make doc

and checkout `_doc/index.html`.


