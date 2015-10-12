(**************************************************************************)
(*  Copyright 2015, Sebastien Mondet <seb@mondet.org>                     *)
(*                                                                        *)
(*  Licensed under the Apache License, Version 2.0 (the "License");       *)
(*  you may not use this file except in compliance with the License.      *)
(*  You may obtain a copy of the License at                               *)
(*                                                                        *)
(*      http://www.apache.org/licenses/LICENSE-2.0                        *)
(*                                                                        *)
(*  Unless required by applicable law or agreed to in writing, software   *)
(*  distributed under the License is distributed on an "AS IS" BASIS,     *)
(*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or       *)
(*  implied.  See the License for the specific language governing         *)
(*  permissions and limitations under the License.                        *)
(**************************************************************************)

(** Implementation of [Trakeva_interface.KEY_VALUE_STORE] with a
    {!Postgresql} backend. *)

(** {3 Implementation of the API}

The function [create] takes a “[conninfo]” URI string,
cf.
{{:http://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING}documentation}.

*)

include Trakeva.KEY_VALUE_STORE

(** {3 Debugging } *)

val debug : bool ref
(** Set [dbug] to [true] to print debug messages on [stderr], this variable
    is also set when {!load} is called while the environment variable
    ["TRAKEVA_SQLITE_DEBUG"] is set with the string ["true"]. *)

val table_name: t -> string
(** The SQL table that is used internally. *)

