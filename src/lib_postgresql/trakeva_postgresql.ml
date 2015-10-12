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

module PG = Postgresql
  
let debug = ref false

let dbg fmt = ksprintf (eprintf "Trakeva_postgresql: %s\n%!") fmt

type t = {
  handle: PG.connection;
  table_name: string;
  conninfo: string;
  action_mutex: Lwt_mutex.t;
}

let dbg_handle {handle} fmt =
  ksprintf (fun s ->
      eprintf "Trakeva_postgresql: %s\n" s;
      eprintf "  db      = %s\n" handle#db;
      eprintf "  user    = %s\n" handle#user;
      eprintf "  pass    = %s\n" handle#pass;
      eprintf "  host    = %s\n" handle#host;
      eprintf "  port    = %s\n" handle#port;
      eprintf "  tty     = %s\n" handle#tty;
      eprintf "  option  = %s\n" handle#options;
      eprintf "  pid     = %i\n" handle#backend_pid
    ) fmt

let in_posix_thread ~on_exn f =
  Lwt_preemptive.detach (fun () ->
  (* (fun f () -> Lwt.return (f ()))  (fun () -> *)
    try `Ok (f ())
    with e -> on_exn e) ()

let create_table t =
  sprintf "CREATE TABLE IF NOT EXISTS %s \
           (collection BYTEA, key BYTEA, value BYTEA, \
           UNIQUE (collection, key))" t
let default_table = "trakeva_default_table"

let table_name t = t.table_name

let load_exn conninfo =
  let handle = new PG.connection ~conninfo () in
  let table_name = default_table in
  let res = handle#exec (create_table table_name) in
  match res#status with
  | PG.Command_ok ->
    let action_mutex = Lwt_mutex.create () in
    {handle; table_name; conninfo; action_mutex}
  | PG.Empty_query 
  | PG.Tuples_ok 
  | PG.Copy_out 
  | PG.Copy_in 
  | PG.Bad_response 
  | PG.Nonfatal_error 
  | PG.Fatal_error 
  | PG.Copy_both 
  | PG.Single_tuple  ->
    ksprintf failwith "Cannot create table %S: %s, %s"
      table_name (PG.result_status res#status) res#error

let exn_to_string = function
  | PG.Error e -> sprintf "Postgres-Error: %s" (PG.string_of_error e)
  | e -> sprintf "Exn: %s" (Printexc.to_string e)

let load conninfo =
  let on_exn e = `Error (`Database (`Load conninfo, exn_to_string e)) in
  in_posix_thread ~on_exn (fun () -> load_exn conninfo)

let close {handle} =
  let on_exn e = `Error (`Database (`Close, exn_to_string e)) in
  in_posix_thread ~on_exn begin fun () ->
    handle#finish
  end

let exec_sql_exn {handle; table_name} sql_list =
  let show_res query args res =
    dbg "\n  %s\n  args: [%s]\n  status: %s | error: %s | tuples: %d × %d"
      query
      (Array.map args ~f:(function
         | `Null -> "NULL"
         | `Blob b -> sprintf "%S" b)
       |> Array.to_list
       |> String.concat ~sep:", ")
      (PG.result_status res#status) res#error res#ntuples res#nfields;
    for i = 0 to res#ntuples - 1 do
      dbg "     (%s)"
        (List.init res#nfields (fun j ->
             if res#getisnull i j then "Null"
             else sprintf "%S" (PG.unescape_bytea (res#getvalue i j)))
         |> String.concat ~sep:", ");
    done;
  in
  let exec_one sql args =
    let res =
      let params =
        Array.map args ~f:(function | `Null -> PG.null | `Blob s -> s) in
      let binary_params =
        Array.map args ~f:(function `Null -> false | `Blob _ -> true) in
      handle#exec sql ~params ~binary_params
    in
    (if !debug then show_res sql  args res);
    begin match res#status with
    | PG.Command_ok -> `Unit
    | PG.Tuples_ok ->
      `Tuples
        (List.init res#ntuples (fun i ->
             (List.init res#nfields (fun j ->
                  if res#getisnull i j then `Null
                  else `Blob (PG.unescape_bytea (res#getvalue i j))))))
    | PG.Empty_query 
    | PG.Copy_out 
    | PG.Copy_in 
    | PG.Bad_response 
    | PG.Nonfatal_error 
    | PG.Fatal_error 
    | PG.Copy_both 
    | PG.Single_tuple  ->
      ksprintf failwith "SQL Query failed (%s): %s, %s"
        sql (PG.result_status res#status) res#error
    end
  in
  let result =
   List.map sql_list ~f:(function
     | (sql, args) ->
       exec_one sql args
     )
  in
  result

let exec_unit t sql args =
  begin match exec_sql_exn t [sql, args] with
  | [`Unit] -> ()
  | other ->
    ksprintf failwith "Unexpected return from “unit query”: %S (length: %d)"
      sql (List.length other)
  end

open Trakeva

let collection_sql_arg collection =
  match collection with None  -> `Null | Some c -> `Blob c
let collection_sql_condition arg =
  sprintf "(collection = %s or (%s is null AND collection is null))" arg arg

let get_exn ?collection t ~key =
  exec_sql_exn t [
    sprintf "SELECT value FROM %s \
             WHERE %s AND key = $2"
      t.table_name (collection_sql_condition "$1"),
    [| collection_sql_arg collection; `Blob key |];
  ] |> function
  | [`Tuples [[`Blob value]]] -> Some value
  | [`Tuples []] -> None
  | other ->
    ksprintf failwith "Did not get one of zero values for (%s, %s)"
      (Option.value collection ~default:"None") key

let get ?collection t ~key =
  let error_loc = (Key_in_collection.create key ?collection) in
  let on_exn e = `Error (`Database (`Get error_loc , exn_to_string e)) in
  in_posix_thread ~on_exn (fun () ->
      get_exn ?collection t ~key
    )


let act t ~(action: Action.t) =
  let rec transact (action: Action.t) =
    let open Key_in_collection in
    let open Action in
    match action with
    | Set ({ key; collection }, value) ->
      exec_unit t 
        (sprintf "UPDATE %s SET value = $3 \
                  WHERE %s AND key = $2"
           t.table_name (collection_sql_condition "$1"))
        [| collection_sql_arg collection; `Blob key; `Blob value |];
      exec_unit t
        (sprintf "INSERT INTO %s (collection, key, value) SELECT $1, $2, $3 \
                  WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s AND key = $2)"
           t.table_name t.table_name (collection_sql_condition "$1"))
        [| collection_sql_arg collection; `Blob key; `Blob value |];
      true
    | Unset { key; collection } ->
      exec_unit t
        (sprintf "DELETE FROM %s WHERE %s AND key = $2"
           t.table_name (collection_sql_condition "$1"))
        [| collection_sql_arg collection; `Blob key; |];
      true
    | Sequence l -> List.for_all l ~f:transact
    | Check ({ key; collection }, opt) ->
      let got_opt = get_exn t ?collection ~key in
      opt = got_opt
  in
  let error_loc = `Act action in
  let on_exn e = `Error (`Database (error_loc , exn_to_string e)) in
  Lwt_mutex.with_lock t.action_mutex (fun () ->
      in_posix_thread ~on_exn begin fun () ->
        exec_unit t "BEGIN" [| |];
        exec_unit t
          (sprintf "LOCK TABLE %s IN ACCESS EXCLUSIVE MODE" t.table_name) [| |];
        begin match transact action with
        | false ->
          exec_unit t "ROLLBACK" [| |];
          `Not_done
        | true ->
          exec_unit t "END" [| |];
          `Done
        end
      end
    )

let get_all_keys_exn t collection =
  exec_sql_exn t [
    sprintf "SELECT key FROM %s WHERE collection = $1 ORDER BY key"
      t.table_name, [| `Blob collection |];
  ] |> function
  | [`Tuples tuples] ->
    List.map tuples ~f:(function
      | [`Blob k] -> k
      | other ->
        ksprintf failwith "Did not get one single row for get_all %S"
          collection)
  | other ->
    ksprintf failwith "Did not get one list of tuples for get_all %S"
      collection

let get_all t ~collection =
  let error_loc = `Get_all collection in
  let on_exn e = `Error (`Database (error_loc , exn_to_string e)) in
  in_posix_thread ~on_exn (fun () -> get_all_keys_exn t collection)


let iterator t ~collection =
  let error_loc = `Iter collection in
  let on_exn e = `Error (`Database (error_loc , exn_to_string e)) in
  let state = ref `Not_started in
  let next_exn remaining_keys =
    match remaining_keys with
    | [] ->
      state := `Closed;
      None
    | head :: tail ->
      state := `Active tail;
      Some head
  in
  begin fun () ->
    in_posix_thread ~on_exn (fun () ->
        match !state with
        | `Not_started ->
          let all_keys = get_all_keys_exn t collection in
          next_exn all_keys
        | `Closed ->
          ksprintf failwith "Iterator already closed: %S" collection
        | `Active remaining_keys ->
          next_exn remaining_keys
      )
  end
