(**************************************************************************)
(*  Copyright 2014, 2015, Sebastien Mondet <seb@mondet.org>               *)
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
module String = Sosa.Native_string 
let (//) = Filename.concat

let say fmt = ksprintf (printf "%s\n%!") fmt

module Test = struct
  exception Tests_failed

  let max_failures = 
    try Sys.getenv "MAX_FAILURES" |> int_of_string with _ -> 2_000_000

  let failed_tests = ref []
  let fail s =
    failed_tests := s :: !failed_tests;
    if List.length !failed_tests > max_failures then (
      List.iter !failed_tests ~f:(fun t ->
          eprintf "Failed test: %S\n%!" t
        );
      raise Tests_failed 
    ) else ()

  let new_tmp_dir () =
    let db_file = Filename.temp_file  "trakeva_tmp_test" ".d" in
    Sys.command (sprintf "rm -rf %s" db_file) |> ignore;
    Sys.command (sprintf "mkdir -p %s" db_file) |> ignore;
    db_file

  let run_monad name f =
    Lwt_main.run (f ())
    |> function
    | `Ok () -> ()
    | `Error (`Database e) ->
      ksprintf fail "%S ends with error: %s" name
        (Trakeva.Error.to_string e)

  let check names c =
    if c then ()
    else ksprintf fail "test.assert failed: %s" (String.concat ~sep:" → " names)

  let now () = Unix.gettimeofday ()
end

  
module type TEST_DATABASE = sig
  val test_name: string
  module DB: Trakeva.KEY_VALUE_STORE
  val debug_mode : bool -> unit
end
module In_memory : TEST_DATABASE = struct

  let test_name = "in-mem"
  let debug_mode _ = ()
  module DB = struct
    type t = {
      nocol: (string, string) Hashtbl.t;
      cols: (string, (string, string) Hashtbl.t) Hashtbl.t;
    }
    let load _ = return {nocol = Hashtbl.create 42; cols = Hashtbl.create 42} 
    let close _ = return ()

    let get_collection t = function
      | None -> t.nocol
      | Some s ->
        begin try Hashtbl.find t.cols s
        with _ ->
          let newone = Hashtbl.create 42 in
          Hashtbl.add t.cols s newone;
          newone
        end

    let get ?collection t ~key =
      let col = get_collection t collection in
      begin try Some (Hashtbl.find col key) |> return 
      with _ -> return None
      end

    let get_all t ~collection =
      let col = get_collection t (Some collection) in
      let l = ref [] in
      Hashtbl.iter (fun k v -> l := k :: !l) col;
      return !l

    let iterator t ~collection =
      let allref = ref None in
      begin fun () ->
        begin match !allref with
        | None ->
          get_all t collection
          >>= fun all ->
          let a = ref all in
          allref := Some a;
          return a
        | Some l -> return l
        end
        >>= fun all ->
        match !all with
        | [] -> return None
        | h :: t -> all := t; return (Some h)
      end

    let act t ~action =
      let open Trakeva.Action in
      let open Trakeva.Key_in_collection in
      let rec go = function
      | Set ({key; collection}, v) ->
        let col = get_collection t collection in
        Hashtbl.replace col key v;
        true
      | Unset {key; collection} ->
        let col = get_collection t collection in
        Hashtbl.remove col key;
        true
      | Check _ -> true
      | Sequence l ->
        List.fold l ~init:true ~f:(fun prev act -> prev && go act)
      in
      match go action with
      | true -> return `Done
      | false -> return `Not_done


  end

end

let open_close_test (module Test_db : TEST_DATABASE) uri_string () =
  let open Test_db in
  DB.load uri_string
  >>= fun db ->
  DB.close db

let basic_test (module Test_db : TEST_DATABASE) uri_string () =
  let open Test_db in
  let open Trakeva.Action in
  let local_assert name c =
    Test.check (name :: test_name :: uri_string :: []) c in
  DB.load uri_string
  >>= fun db ->
  let test_get ?(handle=db) ?collection k f =
    DB.get handle ?collection ~key:k
    >>= fun opt ->
    local_assert (sprintf "get %s/%s" (Option.value collection ~default:"") k)
      (f opt);
    return () in
  let is_none = ((=) None) in
  test_get "k" is_none >>= fun () ->
  test_get ~collection:"c" "k" is_none >>= fun () ->
  DB.get_all db ~collection:"c"
  >>= fun list ->
  local_assert "all c" (list = []);
  let test_actions res actions =
    let action = seq actions in
    DB.act db ~action
    >>= function
    | r when r = res -> return ()
    | `Done ->
      ksprintf Test.fail "Action %s should be Not_done" (to_string action);
      return ()
    | `Not_done ->
      ksprintf Test.fail "Action %s should be Done" (to_string action);
      return ()
  in
  test_actions `Done [set ~key:"k" "v0"] >>= fun () ->
  test_get  "k" ((=) (Some "v0")) >>= fun () ->
  test_actions `Done [set ~key:"k" "v"] >>= fun () ->
  test_get  "k" ((=) (Some "v")) >>= fun () ->
  test_actions `Done [
    contains ~key:"k" "v";
    unset "k";
    set ~key:"k1" ~collection:"c" "V";
  ] >>= fun () ->
  test_actions `Done [
    set ~key:"thekey" ~collection:"c1" "v1";
    set ~key:"thekey" ~collection:"c2" "v2";
  ]
  >>= fun () ->
  test_get ?collection:None "thekey" ((=) None) >>= fun () ->
  test_get ~collection:"c1" "thekey" ((=) (Some "v1")) >>= fun () ->
  test_get ~collection:"c2" "thekey" ((=) (Some "v2")) >>= fun () ->
  test_actions `Not_done [
    contains ~key:"k" "v";
    set ~key:"k1" ~collection:"c" "V";
  ] >>= fun () ->
  test_actions `Done [
    is_not_set "k";
    set ~key:"k2" ~collection:"c" "V2";
    set ~key:"k3" ~collection:"c" "V3";
    set ~key:"k4" ~collection:"c" "V4";
    set ~key:"k5" ~collection:"c" "V5";
  ] >>= fun () ->
  DB.get_all db ~collection:"c"
  >>= fun list ->
  local_assert "full collection 'c'"
    (List.sort ~cmp:String.compare list = ["k1"; "k2"; "k3"; "k4"; "k5"]);
  let key = "K" in
  let insane =
    let buf = Bytes.make 256 '\000' in
    for i = 0 to 255 do Bytes.set buf i (char_of_int i) done;
    Bytes.to_string buf in
  test_actions `Done [
    set ~key "\"";
    set ~key "\\\"";
    set ~key "\"'\"";
    set ~key:insane insane;
  ]
  >>= fun () ->
  test_get ?collection:None insane ((=) (Some insane))
  >>= fun () ->
  let to_list res_stream =
    let rec go acc =
      res_stream ()
      >>= function
      | None -> return acc
      | Some v -> go (v :: acc) in
    go []
  in
  let list_equal l1 l2 =
    let prep = List.sort ~cmp:Pervasives.compare in
    match prep l1 = prep l2 with
    | true -> true
    | false ->
      say "[%s] ≠ [%s]"
        (String.concat  ~sep:", " l1)
        (String.concat  ~sep:", " l2);
      false
  in
  let check_iterator_like_get_all ~collection =
    let stream = DB.iterator db ~collection in
    to_list stream
    >>= fun all ->
    DB.get_all db ~collection
    >>= fun all_too ->
    local_assert (sprintf "iter %s" collection) (list_equal all all_too);
    return ()
  in
  check_iterator_like_get_all "c" >>= fun () ->
  check_iterator_like_get_all "cc" >>= fun () ->
  (*
     We now test going through a collection with `iterator` while
     modifying the values of the collection.
  *)
  let make_self_collection ~collection =
    let keyvalues = List.init 10 ~f:(sprintf "kv%d") in
    let actions = List.map keyvalues ~f:(fun kv -> set ~collection ~key:kv kv) in
    test_actions `Done actions
    >>= fun () ->
    return keyvalues
  in
  let iterate_and_set ~collection =
    let stream = DB.iterator db ~collection in
    let rec go_through () =
      stream () >>= function
      | Some kv ->
        (* say "%s got some %S in the stream" Test_db.test_name kv; *)
        test_actions `Done [set ~collection ~key:kv ("SET" ^ kv)]
        >>= fun () ->
        go_through ()
      | None -> return ()
    in
    go_through ()
  in
  let test_rw_interleave collection =
    make_self_collection ~collection
    >>= fun keyvalues ->
    iterate_and_set ~collection
    >>= fun () ->
    check_iterator_like_get_all collection
    >>= fun () ->
    DB.get_all db ~collection
    >>= fun allnew ->
    (* let modified = List.map keyvalues (fun v -> "SET" ^ v) in *)
    local_assert (sprintf "test_rw_interleave %s" collection)
      (list_equal keyvalues allnew);
    return ()
  in
  (* Test_db.debug_mode true; *)
  test_rw_interleave "ccc"
  >>= fun () ->
  (*
     We now try to delete all values in a collection while iterating on it.
  *)
  let iterate_and_delete ~collection ~at ~all =
    let stream = DB.iterator db ~collection in
    let rec go_through remaining =
      stream () >>= function
      | Some kv when remaining = 0 ->
        say "%s got some %S in the stream" Test_db.test_name kv;
        test_actions `Done (List.map all ~f:(fun key -> unset ~collection key))
        >>= fun () ->
        go_through (-1)
      | Some kv ->
        say "%s got some %S in the stream : remaining: %d" Test_db.test_name kv remaining;
        go_through (remaining - 1)
      | None -> return ()
    in
    go_through at
  in
  let test_delete_iterleave collection =
    make_self_collection ~collection
    >>= fun keyvalues ->
    iterate_and_delete
      ~collection ~all:keyvalues ~at:(List.length keyvalues / 2)
    >>= fun () ->
    DB.get_all db ~collection
    >>= fun allnew ->
    local_assert ("test_delete_iterleave") (allnew = []);
    return ()
  in
  test_delete_iterleave "aaa"
  >>= fun () ->
  (* Read and write concurrently: *)
  let bunch_of_ints = List.init 100 ~f:(fun i -> i) in
  Deferred_list.for_concurrent bunch_of_ints ~f:(function
    | n when n mod 2 = 0 ->
      let v = sprintf "%d" n in
      test_actions `Done [set ~key:v v]
    | n ->
      DB.get db ~key
      >>= fun _ ->
      return ()
    )
  >>= fun ((_ : unit list), errors) ->
  begin match errors with
  | [] -> return ()
  | more ->
    let msg =
      sprintf "concurrent errors: %s"
        (List.map more ~f:(function
           | `Database (`Get _, m)
           | `Database (`Act _, m) -> m)
         |> String.concat ~sep:"; ") in
    local_assert msg false;
    return ()
  end
  >>= fun () ->
  (* Test_db.debug_mode false; *)
  DB.close db

let benchmark_01 (module Test_db : TEST_DATABASE) uri_string
    ?(collection = 200) ?(big_string_kb = 10) () =
  let open Test_db in
  let open Trakeva.Action in
  let benches = ref [] in
  let add_bench s t = benches := (s, t) :: !benches in
  DB.load uri_string
  >>= fun db ->
  let bench_function s f =
    let b = Test.now () in
    f ()
    >>= fun x ->
    let e = Test.now () in
    add_bench s (e -. b);
    return x
  in
  let bench_action ~action s =
    bench_function s (fun () -> DB.act db ~action:(seq action)) in
  let action =
    List.init collection (fun i ->
        set ~key:(sprintf "k%d" i) ~collection:"c" "small")
  in
  bench_action ~action (sprintf "%d small strings" collection)
  >>= fun _ ->
  let bench_in_collection name =
    bench_action
      (sprintf "Set %d %d KB strings into %s" collection big_string_kb name)
      ~action:(
        List.init collection (fun i ->
            let value = String.make (big_string_kb * 1_000) 'B' in
            set ~key:(sprintf "k%d" i) ~collection:name value))
    >>= fun _ ->
    bench_function (sprintf "Get all collection %s" name)
      (fun () -> DB.get_all db name)
    >>= fun cc ->
    let l = List.length cc in
    Test.check ["bench01"; "length"; name; Int.to_string l] (l = collection);
    bench_function (sprintf "Iterate on all collection %s" name)
      (fun () ->
         let iter = DB.iterator db ~collection:name in
         let rec loop () =
           iter ()
           >>= function
           | Some obj -> loop ()
           | None -> return ()
         in
         loop ())
    >>= fun () ->
    bench_function (sprintf "Get all %s one by one" name)
      (fun () ->
         let rec loop n =
           if n = 0
           then return ()
           else (
             let key = sprintf "k%d" (n - 1) in
             DB.get ~collection:name  db ~key
             >>= fun v ->
             Test.check ["bench01"; key; "not none"; ] (v <> None);
             loop (n - 1)
           )
         in
         loop collection)
    >>= fun _ ->
    return ()
  in
  bench_in_collection "C1" >>= fun () ->
  bench_in_collection "C2" >>= fun () ->
  bench_in_collection "C3" >>= fun () ->
  bench_in_collection "C3" >>= fun () ->
  DB.close db
  >>= fun () ->
  return (test_name, List.rev !benches)

  
module Test_sqlite = struct
  let test_name = "Test_sqlite" 
  module DB = Trakeva_sqlite
  let debug_mode v = Trakeva_sqlite.debug := v
end

module Test_sqlite_with_greedy_cache = struct
  let test_name = "Test_sqlite_with_greedy_cache" 
  module DB = Trakeva_cache.Add(Trakeva_sqlite)
  let debug_mode v =
    Trakeva_sqlite.debug := v;
    Trakeva_cache.debug := v;
end

module Test_postgresql = struct
  let test_name = "Test_postgresql" 
  module DB = Trakeva_of_uri
  let debug_mode v = Trakeva_postgresql.debug := v
end

let pg_server () =
  let cmdf fmt =
    ksprintf (fun s ->
        let ret = Sys.command s in
        if ret = 0 then () else failwith (sprintf "command %S returned %d" s ret)
      ) fmt in
  let dir =  "/tmp/trakeva_test/" in
  let port = try Sys.getenv "POSTGRESQL_PORT" |> int_of_string with _ -> 4242 in
  cmdf "rm -fr %s" dir;
  cmdf "mkdir -p %s" dir;
  cmdf "initdb -D %s" dir;
  cmdf "PGPORT=%d pg_ctl start -l %s/log -D %s" port dir dir;
  say "Starting postgresql test server on port %d with %s" port dir;
  let stop () = cmdf "PGPORT=%d pg_ctl -D %s -m fast stop" port dir in
  at_exit stop;
  object
    method stop = stop ()
    method status = cmdf "PGPORT=%d pg_ctl -D %s status" port dir
    method port = port
    method conninfo = sprintf "postgresql:///template1?port=%d" port
  end

let independent_pg_test () =
  let module PG = Postgresql in
  let pg = pg_server () in
  let conninfo = pg#conninfo in
  Unix.sleep 1;
  begin try
    let handle = new PG.connection ~conninfo () in
    let create_table t =
      sprintf "CREATE TABLE IF NOT EXISTS %s \
               (collection BYTEA, key BYTEA, value BYTEA, \
               UNIQUE (collection, key))" t in
    let res = handle#exec (create_table "test") in
    let show_res name res =
      say "%s\n  status: %s | error: %s | tuples: %d × %d"
        name (PG.result_status res#status) res#error res#ntuples res#nfields;
      for i = 0 to res#ntuples - 1 do
        say "     (%s)"
          (List.init res#nfields (fun j ->
               if res#getisnull i j then "Null"
               else sprintf "%S" (PG.unescape_bytea (res#getvalue i j)))
           |> String.concat ~sep:", ");
      done;
    in
    show_res "create-table" res;
    let res = handle#exec (create_table "test") in
    show_res "create-table again" res;
    let execl l =
      List.iter l ~f:(function
        | (sql, args) ->
          let res =
            let as_array = Array.of_list args in
            let params =
              Array.map as_array ~f:(function | `Null -> PG.null | `V s -> s) in
            let binary_params =
              Array.map as_array ~f:(function `Null -> false | `V _ -> true) in
            handle#exec sql ~params ~binary_params
          in
          let name = String.split sql ~on:(`Character '\n') |> List.hd_exn in
          show_res name res
        )
    in
    let add c k v =
      execl [
        "BEGIN", [];
        "LOCK TABLE test IN ACCESS EXCLUSIVE MODE", [];
        "UPDATE test SET value = $3 WHERE collection = $1 AND key = $2", [c; `V k; `V v];
        {sql|
           INSERT INTO test (collection, key, value)
                  SELECT $1, $2, $3
                  WHERE NOT EXISTS (SELECT 1 FROM test WHERE collection = $1 AND key = $2);
      |sql}, [c; `V k; `V v];
        "END", [];
      ] in
    add (`V "C") "K1" "V1";
    add (`V "C") "K2" "V2";
    add (`V "C") "K3" "V3\000\001";
    add (`V "C") "K2" "V4";
    execl ["SELECT key, value FROM test WHERE collection = $1", [`V "C"];];
    execl [
      "DELETE FROM test WHERE key = $2 AND collection = $1", [`V "C"; `V "K1"];
    ];
    execl ["SELECT collection, key, value FROM test WHERE collection = $1", [`V "C"];];
    add (`Null) "K1" "VVVVVV";
    add (`V "null") "K2" "VVVvvvvvvvVVV";
    execl ["SELECT collection, key, value FROM test WHERE collection = $1", [`Null];];
    execl ["SELECT collection, key, value FROM test WHERE collection is null", [];];
    execl ["SELECT collection, key, value FROM test WHERE collection = $1 or ($1 is null AND collection is null)", [`Null];];
    execl ["SELECT collection, key, value FROM test WHERE collection = $1 or ($1 is null AND collection is null)", [`V "C"];];
    begin
      Test.run_monad "pgtest" (fun () ->
          Trakeva_postgresql.load conninfo
          >>= fun trak ->
          Trakeva_postgresql.get trak ~key:"K"
          >>= begin function
          | None -> say "get OK"; return ()
          | Some s -> say "what??? %S" s; return ()
          end
          >>= fun () ->
          let open Trakeva.Action in
          Trakeva_postgresql.act trak  (seq [
              set ~key:"K" "VVV";
              set ~collection:"C" ~key:"K" "VVV";
              set ~key:"K1" "VVV";
              unset "K1";
              is_not_set "K1";
            ])
          >>= fun _ ->
          Trakeva_postgresql.get trak ~key:"K"
          >>= begin function
          | None -> say "2nd get NOT OK"; return ()
          | Some s -> say "OK: %S" s; return ()
          end
          >>= fun () ->
          let tab = Trakeva_postgresql.table_name trak in
          execl [sprintf "SELECT collection, key, value FROM %s WHERE collection = $1 or ($1 is null AND collection is null)" tab, [`Null];];
          Trakeva_postgresql.act trak  (seq [
              set ~collection:"C" ~key:"K"  "\000";
              set ~collection:"C" ~key:"K1" "\000";
              set ~collection:"C" ~key:"K2" "\000";
              set ~collection:"C" ~key:"K3" "\000";
            ])
          >>= fun _ ->
          Trakeva_postgresql.get_all trak ~collection:"C"
          >>= fun list_of_keys ->
          say "list_of_keys: %s" (String.concat ~sep:", " list_of_keys);
          execl [sprintf "SELECT collection, key, value FROM %s WHERE collection = $1 or ($1 is null AND collection is null)" tab, [`V "C"];];
          let it = Trakeva_postgresql.iterator trak ~collection:"C" in
          let rec loop () =
            it ()
            >>= function
            | Some key ->
              Trakeva_postgresql.get trak ~collection:"C" ~key
              >>= fun v ->
              say "%s -> %s" key (Option.value v ~default:"????");
              loop ()
            | None ->
              say "Iterator done";
              return ()
          in
          loop ()
          >>= fun () ->
          (*
          execl ["BEGIN", []];
          execl [sprintf "DECLARE cur1 CURSOR FOR (SELECT collection, key, value FROM %s WHERE collection = $1 or ($1 is null AND collection is null))" tab, [`V "C"];];
          (* execl ["OPEN cur1", []]; *)
          execl ["FETCH NEXT FROM cur1", []];
          execl ["FETCH NEXT FROM cur1", []];
          execl ["FETCH NEXT FROM cur1", []];
          execl ["FETCH NEXT FROM cur1", []];
          execl ["FETCH NEXT FROM cur1", []];
          execl ["FETCH NEXT FROM cur1", []];
          execl ["CLOSE cur1", []];
          execl ["END", []];
             *)
          return ()
        )
    end;
    handle#finish;
  with
  | PG.Error e -> say "Error: %s" (PG.string_of_error e)
  | e -> say "Exn: %s" (Printexc.to_string e)
  end;
  say "EXITING";
  exit 0
(*
let () = pg#stop 
let () = pg#status
*)

let has_arg arlg possible =
  List.exists arlg ~f:(List.mem ~set:possible)

let find_arg argl ~name ~convert =
  List.find_map argl (fun arg ->
      match String.split ~on:(`Character '=') arg |> List.map ~f:String.strip with
      | n :: c :: [] when n = name -> convert c
      | _ -> None)

let () =
  let argl = Sys.argv |> Array.to_list |> List.tl_exn in

  (* independent_pg_test (); *)
  
  let sqlite_path = "/tmp/trakeva-sqlite-test" in
  ksprintf Sys.command "rm -fr %s" sqlite_path |> ignore;
  Test.run_monad "basic/sqlite" (basic_test (module Test_sqlite) sqlite_path);

  ksprintf Sys.command "rm -fr %s" sqlite_path |> ignore;
  Test.run_monad "basic/sqlite-with-cache" (basic_test (module Test_sqlite_with_greedy_cache) sqlite_path);

  begin
    let conninfo =
      try Sys.getenv "POSTGRESQL_CONNECTION_INFO"
      with _ ->
        let pg = pg_server () in
        Unix.sleep 1;
        pg#conninfo
    in
    Test.run_monad "basic/postgres" (basic_test (module Test_postgresql) conninfo);
    (* pg#stop; *)
  end;

  if has_arg argl ["bench"; "benchmarks"] then (
    ksprintf Sys.command "rm -fr %s" sqlite_path |> ignore;
    let collection = find_arg argl ~name:"collection" ~convert:Int.of_string in
    let big_string_kb = find_arg argl ~name:"kb" ~convert:Int.of_string in
    let timeout =
      find_arg argl ~name:"timeout" ~convert:Float.of_string
      |> Option.value ~default:10.
    in
    let bench_with_timeout file f =
      ksprintf Sys.command "rm -fr %s" sqlite_path |> ignore;
      System.with_timeout timeout (fun () -> f file)
      >>< begin function
      | `Ok (_, res) -> return res
      | `Error (`Timeout t) -> return ["Timeout", t]
      | `Error (`System (_, `Exn e)) -> raise e
      | `Error (`Database _ as other) -> fail other
      end
    in
    Test.run_monad "bench01" (fun () ->
        bench_with_timeout "/tmp/trakeva-bench-sqlite" (fun path ->
            benchmark_01 ?collection ?big_string_kb
              (module Test_sqlite) path ()
          )
        >>= fun sqlite_bench01 ->
        bench_with_timeout "/tmp/trakeva-bench-sqlite-with-cache" (fun path ->
            benchmark_01 ?collection ?big_string_kb
              (module Test_sqlite_with_greedy_cache) path ()
          )
        >>= fun sqlite_cached_bench01 ->
        bench_with_timeout "" (fun _ ->
            benchmark_01 ?collection ?big_string_kb
              (module In_memory) "" ()
          )
        >>= fun in_mem_bench01 ->
        say "Bench01 sqlite: %F\tsqlite+cache: %F\tin-mem: %F"
          (List.fold sqlite_bench01 ~init:0. ~f:(fun p (_, f) -> p +. f))
          (List.fold sqlite_cached_bench01 ~init:0. ~f:(fun p (_, f) -> p +. f))
          (List.fold in_mem_bench01 ~init:0. ~f:(fun p (_, f) -> p +. f)) ;
        List.iter sqlite_bench01 ~f:(fun (n, f) ->
            say "sqlite\t%s\t%F s" n f
          );
        List.iter sqlite_cached_bench01 ~f:(fun (n, f) ->
            say "sqliteGC\t%s\t%F s" n f
          );
        List.iter in_mem_bench01 ~f:(fun (n, f) ->
            say "in_mem\t%s\t%F s" n f
          );
        return ()
      );
  );
  begin match !Test.failed_tests with
  | [] ->
    say "No tests failed \\o/ (arg-list: [%s])"
      (String.concat ~sep:"." (List.map argl ~f:(sprintf "%S")));
    exit 0
  | some ->
    say "Some tests failed (arg-list: [%s]):"
      (String.concat ~sep:"." (List.map argl ~f:(sprintf "%S")));
    List.iter some ~f:(fun t ->
        say "%s" t;
        say "%s" (String.make 80 '-');
      );
    exit 3
  end
