(**************************************************************************)
(*  Copyright 2014, Sebastien Mondet <seb@mondet.org>                     *)
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

open Nonstd
open Pvem_lwt_unix
open Pvem_lwt_unix.Deferred_result
module String = StringLabels

module Cache = struct

  type collection = {
    hashtbl: (string, string option) Hashtbl.t;
  }
  module String_map = Map.Make(String)
  type t = {
    mutable map: collection String_map.t;
  }
  let create () = { map = String_map.empty; }
  let add_or_replace t ?(collection="") ~key v =
    try
      let {hashtbl} = String_map.find collection t.map in
      Hashtbl.replace hashtbl key v
    with
    | _ ->
      let hashtbl = Hashtbl.create 42 in
      Hashtbl.replace hashtbl key v;
      t.map <- String_map.add collection {hashtbl} t.map;
      ()

  let get t ?(collection="") ~key = 
    try
      let {hashtbl} = String_map.find collection t.map in
      `Set (Hashtbl.find hashtbl key)
    with _ -> `Unset

  let remove t ?(collection="") ~key =
    try
      let {hashtbl} = String_map.find collection t.map in
      Hashtbl.remove hashtbl key
    with _ -> ()

  let get_collection t ~collection ~get_all =
    try
      let col = String_map.find collection t.map in
      return col
    with
    | _ ->
      get_all ()
      >>= fun all ->
      let col = {hashtbl = Hashtbl.create 42} in
      List.iter all ~f:(fun v -> assert false); (* we don't have keys! *)
      t.map <- String_map.add collection col t.map;
      return col

    
end


module Add (KV_DB: Trakeva.KEY_VALUE_STORE): Trakeva.KEY_VALUE_STORE = struct

  type t = {
    original: KV_DB.t;
    cache: Cache.t;
    mutex: Lwt_mutex.t;
  }
  let load s =
    KV_DB.load s
    >>= fun original ->
    return {original; cache = Cache.create (); mutex = Lwt_mutex.create ()}

  let close {original; cache} =
    KV_DB.close original

  open Trakeva.Action
  open Trakeva.Key_in_collection
  let _key ?collection key = {key; collection}

  let get ?collection t ~key =
    Lwt_mutex.with_lock t.mutex (fun () -> 
        match Cache.get t.cache ?collection ~key with
        | `Set v -> return v
        | `Unset ->
          KV_DB.get ?collection t.original ~key
          >>= fun v ->
          Cache.add_or_replace t.cache ?collection ~key v;
          return v
      )

  let get_all t ~collection =
    assert false

  let iterator t ~collection =
    assert false

  let act t ~action =
    assert false

end
