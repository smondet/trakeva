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

(** A key-value database API with basic transactions. *)

open Pvem_lwt_unix
open Nonstd

module Key_in_collection : sig
  type t = {key: string ; collection: string option}
  (** Represent keys that may optionally belong to a collection. *)

  val create : ?collection:string -> string -> t
  val to_string : t -> string
end

module Action : sig

  type t =
    | Set of Key_in_collection.t * string
    | Unset of Key_in_collection.t
    | Sequence of t list
    | Check of Key_in_collection.t * string option
    (** An action is a transaction that (attempts) to modify the database. *)

  val set : ?collection:string -> key:string -> string -> t
  (** Create a “set” action: [set ~key v] will add or set the value [v] for the
      key [key], optionally in the collection [collection]. *)

  val seq : t list -> t
  (** Put a sequence of actions into a transaction. *)

  val contains : ?collection:string -> key:string -> string -> t
  (** An action that checks that the [key] is set in the DB and has the given
      value. *)

  val is_not_set : ?collection:string -> string -> t
  (** An action that checks that the [key] is not set in the DB. *)

  val unset: ?collection:string -> string -> t
  (** An actions that removes a value from the DB. *)

  val to_string: t -> string
  (** Convert the action to a display friendly string (the
      implementation is quite naive and not tail-recursive, hence avoid
      displaying huge transaction). *)
end

module Error : sig
  type t = [
    | `Act of Action.t | `Get of Key_in_collection.t | `Get_all of string
    | `Iter of string
    | `Load of string | `Close
  ] * string
    (** Merge of the possible errors. *)

  val to_string: t -> string
end

module type KEY_VALUE_STORE = sig
  type t
  (** The handle to the database. *)

  val load :
    string ->
    (t, [> `Database of [> `Load of string ] * string ]) Deferred_result.t
  (** Load a handle from the given database parameters. *)

  val close: t ->
    (unit, [> `Database of [> `Close ] * string ]) Deferred_result.t
  (** Close the DB. *)

  val get : ?collection:string -> t -> key:string ->
    (string option, [> `Database of [> `Get of Key_in_collection.t ] * string ])
      Deferred_result.t
  (** Get a value in the DB. *)

  val get_all: t -> collection:string ->
    (string list, [> `Database of [> `Get_all of string ] * string ])
      Deferred_result.t
  (** Get all the keys in a given collection as a list. *)

  val iterator: t -> collection:string ->
    (unit ->
     (string option, [> `Database of [> `Iter of string ] * string ]) Deferred_result.t)
  (** Go through all the kets in a given collection (exact semantics
      versus concurrent writes still to be defined …). *)

  val act :
    t ->
    action:Action.t ->
    ([ `Done | `Not_done ],
     [> `Database of [> `Act of Action.t ] * string ]) Deferred_result.t
  (** Process a transaction, which can be [`Done] is successful or [`Not_done]
      if one of the checks in the [Action.t] failed. *)

end
