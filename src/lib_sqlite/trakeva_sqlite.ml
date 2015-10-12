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

let dbg fmt = ksprintf (eprintf "Trakeva_sqlite: %s\n%!") fmt
    
type t = {
  handle: Sqlite3.db;
  action_mutex: Lwt_mutex.t;
}

let in_posix_thread ~on_exn f =
  Lwt_preemptive.detach (fun () ->
  (* (fun f () -> Lwt.return (f ()))  (fun () -> *)
    try `Ok (f ())
    with e -> on_exn e) ()

(*
let escape_blob s = 
  let b = Buffer.create (String.length s + 4) in
  Buffer.add_char b '\'';
  String.iter s ~f:(function
    | '\'' -> Buffer.add_string b "''"
    | other -> Buffer.add_char b other
    );
  Buffer.add_char b '\'';
  Buffer.contents b
*)    
let escape_blob s = 
  let b = Buffer.create (String.length s * 2 + 4) in
  Buffer.add_string b "X'";
  String.iter s ~f:(fun c ->
      Buffer.add_string b (sprintf "%02X" (int_of_char c));
    );
  Buffer.add_char b '\'';
  Buffer.contents b
    

let default_table = "trakeva_default_table"

(* https://www.sqlite.org/lang_createtable.html *)
let create_table t =
  sprintf "CREATE TABLE IF NOT EXISTS %s \
           (collection BLOB, key BLOB, value BLOB, \
           PRIMARY KEY(collection, key) ON CONFLICT REPLACE)" t

let option_equals =
  function
  | None -> "IS NULL"
  | Some s -> sprintf "= %s" (escape_blob s)

let get_statement table collection key =
  sprintf "SELECT value FROM %s WHERE key = %s AND collection %s" table
     (escape_blob key)
     (option_equals collection)

let get_all_statement table collection =
  sprintf "SELECT key FROM %s WHERE collection %s ORDER BY key" table
    (option_equals collection)

let keys_statement table collection =
  sprintf "SELECT DISTINCT key FROM %s WHERE collection %s"
    table (option_equals collection)

(*
https://www.sqlite.org/lang_insert.html
*)
let set_statement table collection key value =
  sprintf "INSERT OR REPLACE INTO %S (collection, key, value) VALUES (%s, %s, %s)"
    table
    (match collection with None -> "NULL" | Some s -> escape_blob s)
    (escape_blob key) (escape_blob value)

(* https://www.sqlite.org/lang_delete.html *)
let unset_statement table collection key =
  sprintf "DELETE FROM %s WHERE key = %s AND collection %s" table
    (escape_blob key)
    (option_equals collection)

let with_executed_statement handle statement f =
  let prep = Sqlite3.prepare handle statement in
  if !debug then
    dbg "exec: %S\n    → counts: %d, %d"
      statement (Sqlite3.column_count prep) (Sqlite3.data_count prep);
  (try
    let x = f prep in
    let _ = Sqlite3.finalize prep in
    x
  with e ->
    let _ = Sqlite3.finalize prep in
    raise e)

let is_ok_or_done_exn handle (rc: Sqlite3.Rc.t) =
  let open Sqlite3.Rc in
  match rc with
  | OK  -> ()
  | DONE -> ()
  | _ -> failwith (sprintf "not ok/done: %s (global error: %s)"
                     (Sqlite3.Rc.to_string rc)
                     (Sqlite3.errmsg handle))

let get_row_exn prep =
  match Sqlite3.step prep with
  | Sqlite3.Rc.ROW -> Sqlite3.column prep 0, true
  | Sqlite3.Rc.DONE -> Sqlite3.column prep 0, false
  | rc -> failwith (sprintf "not a row: %s" (Sqlite3.Rc.to_string rc))

let exec_unit_exn handle statement =
  with_executed_statement handle statement (fun prep ->
      Sqlite3.step prep |> is_ok_or_done_exn handle)

let string_option_data_exn data =
  let open Sqlite3.Data in
  begin match data with
  | NONE  -> failwith "string_option_data_exn: none"
  | NULL  -> None
  | INT _ -> failwith "string_option_data_exn: int"
  | FLOAT _ -> failwith "string_option_data_exn: float"
  | TEXT s
  | BLOB s -> Some s
  end
  
let exec_option_exn handle statement =
  with_executed_statement handle statement (fun prep ->
      let first, (_ : bool) = get_row_exn prep in
      string_option_data_exn first)

let exec_list_exn handle statement =
  with_executed_statement handle statement begin fun prep ->
    let ret = ref [] in
    let rec loop () =
      let row, more_to_come  = get_row_exn prep in
      match string_option_data_exn row with
      | Some one -> ret:= one :: !ret;
        if more_to_come then loop () else ()
      | None -> ()
    in
    loop ();
    !ret
  end

let load path =
  let on_exn e = `Error (`Database (`Load path, Printexc.to_string e)) in
  begin
    try if Sys.getenv "TRAKEVA_SQLITE_DEBUG" = "true" then debug := true
    with _ -> ()
  end;
  let action_mutex = Lwt_mutex.create () in
  in_posix_thread ~on_exn (fun () ->
      if !debug then
        dbg "openning: %S" path;
      (* we need the mutex `FULL: https://www.sqlite.org/threadsafe.html
         the private cache is up for debate: 
         https://www.sqlite.org/sharedcache.html *)
      let handle = Sqlite3.db_open ~mutex:`FULL ~cache:`PRIVATE path in
      exec_unit_exn handle (create_table default_table);
      {handle; action_mutex}
    )

let close {handle} =
  let on_exn e = `Error (`Database (`Close, Printexc.to_string e)) in
  in_posix_thread ~on_exn begin fun () ->
    let rec loop = function
    | 0 -> failwith "failed to close (busy many times)"
    | n ->
      if Sqlite3.db_close handle then () else (
        Sqlite3.sleep 100 |> ignore;
        if !debug then
          dbg "closing, %d attempts left" (n - 1);
        loop (n - 1)
      )
    in
    loop 5
  end

open Trakeva

let get ?collection t ~key =
  let statement = get_statement default_table collection key in
  let error_loc = `Get (Key_in_collection.create key ?collection) in
  let on_exn e = `Error (`Database (error_loc , Printexc.to_string e)) in
  in_posix_thread ~on_exn (fun () ->
      exec_option_exn t.handle statement
    )

let get_all t ~collection =
  let statement = get_all_statement default_table (Some collection) in
  let error_loc = `Get_all collection in
  let on_exn e = `Error (`Database (error_loc , Printexc.to_string e)) in
  in_posix_thread ~on_exn (fun () ->
      exec_list_exn t.handle statement
    )

let iterator t ~collection =
  let keys_statement = keys_statement default_table (Some collection) in
  let error_loc = `Iter collection in
  let on_exn e = `Error (`Database (error_loc , Printexc.to_string e)) in
  let state = ref None in
  let rec next_exn prep =
    if !debug then
      dbg "exec: %S\n    → counts: %d, %d"
        keys_statement (Sqlite3.column_count prep) (Sqlite3.data_count prep);
    try
      let row, more_to_come  = get_row_exn prep in
      begin match string_option_data_exn row with
      | Some one_key -> Some one_key
      | None ->
        let _ = Sqlite3.finalize prep in
        None
      end
    with e ->
      let _ = Sqlite3.finalize prep in
      raise e
  in
  begin fun () ->
    in_posix_thread ~on_exn (fun () ->
        match !state with
        | None ->
          let prep = Sqlite3.prepare t.handle keys_statement in
          begin match Sqlite3.prepare_tail prep with
          | None  -> state := Some prep;
          | Some p -> state := Some p;
          end;
          next_exn prep
        | Some prep ->
          next_exn prep
      )
  end


let act t ~(action: Action.t) =
  let rec transact (action: Action.t) =
    let open Key_in_collection in
    let open Action in
    match action with
    | Set ({ key; collection }, value) ->
      let statement = set_statement default_table collection key value in
      exec_unit_exn t.handle statement;
      true
    | Unset { key; collection } ->
      let statement = unset_statement default_table collection key in
      exec_unit_exn t.handle statement;
      true
    | Sequence l -> List.for_all l ~f:transact
    | Check ({ key; collection }, opt) ->
      let statement = get_statement default_table collection key in
      exec_option_exn t.handle statement = opt
  in
  let error_loc = `Act action in
  let on_exn e = `Error (`Database (error_loc , Printexc.to_string e)) in
  Lwt_mutex.with_lock t.action_mutex (fun () -> 
      in_posix_thread ~on_exn begin fun () ->
        exec_unit_exn t.handle "BEGIN TRANSACTION";
        begin match transact action with
        | false ->
          exec_unit_exn t.handle "ROLLBACK";
          `Not_done
        | true ->
          exec_unit_exn t.handle "COMMIT";
          `Done
        end
      end
    )
