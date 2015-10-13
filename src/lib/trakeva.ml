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

module Key_in_collection = struct
  type t = {key: string ; collection: string option}
  let create ?collection key = {key; collection}

  let to_string {key; collection} =
    sprintf "{%s/%s}" Option.(value collection ~default:"") key

end

module Action = struct

  type t =
    | Set of Key_in_collection.t * string
    | Unset of Key_in_collection.t
    | Sequence of t list
    | Check of Key_in_collection.t * string option
  let _key ?collection key = {Key_in_collection. key; collection}
  let set ?collection ~key value = Set (_key ?collection key, value)
  let seq l = Sequence l
  let contains ?collection ~key v = Check (_key ?collection key, Some v)
  let is_not_set ?collection key = Check (_key ?collection key, None)
  let unset ?collection key = Unset (_key ?collection key)
  let rec to_string (t: t) =
    match t with
    | Set (k, v) -> sprintf "(set %s %S)" (Key_in_collection.to_string k) v
    | Unset k -> sprintf "(unset %s)" (Key_in_collection.to_string k)
    | Sequence l -> sprintf "(sequence %s)" (List.map l ~f:to_string
                                             |> StringLabels.concat ~sep:" ")
    | Check (k, v) ->
      sprintf "(check %s %s)" (Key_in_collection.to_string k)
        (Option.value_map ~default:"None" v ~f:(sprintf "(Some %S)"))

end

module Error = struct
  type t = [
    | `Act of Action.t | `Get of Key_in_collection.t | `Get_all of string
    | `Load of string | `Close
    | `Iter of string
  ] * string

  let to_string (t : t) =
    match t with
    | (`Act k, e) -> sprintf "[Executing %s, Error: %s]" (Action.to_string k) e
    | (`Close, e) -> sprintf "[Closing, %s]" e
    | (`Get k, e) ->
      sprintf "[Getting %s, Error: %s]" (Key_in_collection.to_string k) e
    | (`Get_all c, e) -> sprintf "[Getting-all-in %s, Error: %s]" c e
    | (`Load u, e) -> sprintf "[Loading %S, Error: %s]" u e
    | (`Iter s, e) -> sprintf "[Iterating on %S, Error: %s]" s e
end

module type KEY_VALUE_STORE = sig
  type t

  val load :
    string ->
    (t, [> `Database of [> `Load of string ] * string ]) Deferred_result.t

  val close: t ->
    (unit, [> `Database of [> `Close ] * string ]) Deferred_result.t

  val get : ?collection:string -> t -> key:string ->
    (string option, [> `Database of [> `Get of Key_in_collection.t ] * string ])
      Deferred_result.t

  val get_all: t -> collection:string ->
    (string list, [> `Database of [> `Get_all of string ] * string ])
      Deferred_result.t

  val iterator: t -> collection:string ->
    (unit ->
     (string option, [> `Database of [> `Iter of string ] * string ]) Deferred_result.t)

  val act :
    t ->
    action:Action.t ->
    ([ `Done | `Not_done ],
     [> `Database of [> `Act of Action.t ] * string ]) Deferred_result.t

end
