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

open Nonstd
open Pvem_lwt_unix
open Pvem_lwt_unix.Deferred_result
module String = StringLabels

let debug = ref false

let dbg fmt = ksprintf (eprintf "Trakeva_cache: %s\n%!") fmt

module Greedy_cache = struct

  type t = {
    collections: (string, (string, string) Hashtbl.t) Hashtbl.t;
  }
  let create () = {collections = Hashtbl.create 10}
  let find_collection {collections} c =
    try Some (Hashtbl.find collections c)
    with _ -> None

  let from_collection c key =
    try Some (Hashtbl.find c key)
    with _ -> None

  let new_collection t collection =
    let col = Hashtbl.create 42 in
    Hashtbl.add t.collections collection col;
    col

  let add_to_collection t col ~key ~value =
    Hashtbl.add col key value

  let fold_collection col ~init ~f =
    let r = ref init in
    Hashtbl.iter (fun key value ->
        if !debug then dbg "fold_collection %s, %s" key value;
        r := f !r ~key ~value) col;
    !r



end


module Add (KV_DB: Trakeva.KEY_VALUE_STORE): Trakeva.KEY_VALUE_STORE = struct

  let default_collection_name = "trakeva-greedy-cache-default-collection"

  type t = {
    original: KV_DB.t;
    cache: Greedy_cache.t;
    mutex: Lwt_mutex.t;
  }
  let load s =
    KV_DB.load s
    >>= fun original ->
    return {original; cache = Greedy_cache.create (); mutex = Lwt_mutex.create ()}

  let close {original; _} =
    KV_DB.close original

  open Trakeva.Action
  open Trakeva.Key_in_collection

  let cache_collection t ~collection =
    if !debug then dbg "cache_collection %s" collection;
    match Greedy_cache.find_collection t.cache collection with
    | Some s -> return s
    | None ->
      if !debug then dbg "cache_collection %s: get_all" collection;
      KV_DB.get_all t.original collection
      >>= fun all_keys ->
      let col = Greedy_cache.new_collection t.cache collection in
      Deferred_list.while_sequential all_keys ~f:(fun key ->
          KV_DB.get t.original ~collection ~key
          >>= function
          | Some value ->
            if !debug then dbg "cache_collection %s: add %s → %s" collection key value;
            Greedy_cache.add_to_collection t.cache col ~key ~value;
            return ()
          | None -> return ()
        )
      >>= fun (_ : unit list) ->
      return col

  let get ?(collection = default_collection_name) t ~key =
    Lwt_mutex.with_lock t.mutex (fun () ->
        cache_collection t ~collection
        >>= fun col ->
        return (Greedy_cache.from_collection col key)
      )
    >>< function
    | `Ok o -> return o
    | `Error (`Database (`Get_all _, s_)) ->
      fail (`Database (`Get (create ~collection key), s_))
    | `Error (`Database (`Get _,_) as e) -> fail e

  let get_all t ~collection =
    Lwt_mutex.with_lock t.mutex (fun () ->
        cache_collection t ~collection
        >>= fun col ->
        if !debug then dbg "get_all %s" collection;
        return (Greedy_cache.fold_collection col ~init:[]
                  ~f:(fun prev ~key ~value -> key :: prev))
      )
    >>< function
    | `Ok o -> return o
    | `Error (`Database (`Get_all _,_) as e) -> fail e
    | `Error (`Database (`Get { key; _ }, s_)) ->
      fail (`Database (`Get_all collection, sprintf "getting %s: %s" key s_))

  let iterator t ~collection =
    let all = ref None in
    begin fun () ->
      begin
        begin match !all with
        | None ->
          get_all t ~collection
          >>= fun all_keys ->
          all := Some all_keys;
          return all_keys
        | Some l -> return l
        end
        >>= function
        | key :: more_keys ->
          all := Some more_keys;
          return (Some key)
        | [] -> return None
      end
      >>< function
      | `Ok o -> return o
      | `Error (`Database (`Get_all _, s_)) ->
        fail (`Database (`Iter collection, sprintf "get-all %s: %s" collection s_))
      | `Error (`Database (`Get { key; _ }, s_)) ->
        fail (`Database (`Iter collection, sprintf "getting %s: %s" key s_))
    end

  let act_in_cache t ~action =
    let open Trakeva.Action in
    let open Trakeva.Key_in_collection in
    let actual_collection = Option.value ~default:default_collection_name in
    let rec go = function
    | Set ({key; collection}, v) ->
      cache_collection t (actual_collection collection)
      >>= fun col ->
      Hashtbl.replace col key v;
      return ()
    | Unset {key; collection} ->
      cache_collection t (actual_collection collection)
      >>= fun col ->
      Hashtbl.remove col key;
      return ()
    | Check _ -> return ()
    | Sequence l ->
      Deferred_list.while_sequential l ~f:(fun act -> go act)
      >>= fun _ ->
      return ()
    in
    go action

  let act t ~action =
    begin
      KV_DB.act t.original ~action
      >>= function
      | `Done ->
        act_in_cache t ~action
        >>= fun () ->
        return `Done
      | `Not_done -> return `Not_done
    end >>< function
    | `Ok o -> return o
    | `Error (`Database (`Get_all collection, s_)) ->
      fail (`Database (`Act action, sprintf "get-all %s: %s" collection s_))
    | `Error (`Database (`Get { key; _ }, s_)) ->
      fail (`Database (`Act action, sprintf "getting %s: %s" key s_))
    | `Error (`Database (`Act _,_) as e) -> fail e

end
