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
  test_actions `Done [set ~key:"k" "v"] >>= fun () ->
  test_get  "k" ((=) (Some "v")) >>= fun () ->
  test_actions `Done [
    contains ~key:"k" "v";
    unset "k";
    set ~key:"k1" ~collection:"c" "V";
  ] >>= fun () ->
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
    (List.sort ~cmp:String.compare list = ["V"; "V2"; "V3"; "V4"; "V5"]);
  let key = "K" in
  test_actions `Done [
    set ~key "\"";
    set ~key "\\\"";
    set ~key "\"'\"";
  ]
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
    let modified = List.map keyvalues (fun v -> "SET" ^ v) in
    local_assert (sprintf "test_rw_interleave %s" collection)
      (list_equal modified allnew);
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
  bench_action (sprintf "%d %d KB strings" collection big_string_kb) ~action:(
    List.init collection (fun i ->
        let value = String.make (big_string_kb * 1_000) 'B' in
        set ~key:(sprintf "k%d" i) ~collection:"cc" value))
  >>= fun _ ->
  bench_function "Get all collection 'cc'" (fun () -> DB.get_all db "cc")
  >>= fun cc ->
  let l = List.length cc in
  Test.check ["bench01"; "length cc"; Int.to_string l] (l = collection);
  bench_function "Get all 'cc' one by one" (fun () ->
      let rec loop n =
        if n = 0
        then return ()
        else (
          let key = sprintf "k%d" (n - 1) in
          DB.get ~collection:"cc"  db ~key
          >>= fun v ->
          Test.check ["bench01"; key; "not none"; ] (v <> None);
          loop (n - 1)
        )
      in
      loop collection)
  >>= fun _ ->
  DB.close db
  >>= fun () ->
  return (test_name, List.rev !benches)

  
let git_db_test () =
  let module DB = Trakeva_git_commands in
  let open Trakeva.Action in
  let db_file  = Test.new_tmp_dir () in
  DB.load db_file
  >>= fun db ->
  DB.get db ~key:"k"
  >>= fun res ->
  begin match res with
  | None -> return ()
  | Some v -> Test.fail (sprintf "key k got %S" v); return ()
  end
  >>= fun () ->
  begin DB.act db DB.(seq [ is_not_set "k"; set ~key:"k" "V" ])
    >>= function
    | `Done -> return ()
    | `Not_done -> Test.fail "seq 1 not done"; return ()
  end
  >>= fun () ->
  let check_k current_db =
    begin DB.get current_db ~key:"k" >>= function
      | Some v when v = "V" -> return ()
      | None -> Test.fail (sprintf "get k got None"); return ()
      | Some v -> Test.fail (sprintf "get k got %S" v); return ()
    end
  in
  check_k db >>= fun () ->
  DB.close db >>= fun () ->
  DB.load db_file
  >>= fun db2 ->
  check_k db2 >>= fun () ->
  begin DB.act db2 DB.(seq [contains ~key:"k" "V"])
    >>= function
    | `Done -> return ()
    | `Not_done -> Test.fail "seq 2 not done"; return ()
  end
  >>= fun () ->
  (* Transation that fails: *)
  begin DB.act db2 DB.(seq [
      set ~key:"k2" "vvv";
      set ~collection:"c3" ~key:"k3" "vvv";
      set ~key:"k2" "uuu";
      contains ~key:"k" "u"])
    >>= function
    | `Not_done -> return ()
    | `Done -> Test.fail "seq 3 done"; return ()
  end
  >>= fun () ->
  (* Transation that succeeds: *)
  begin DB.act db2 DB.(seq [
      is_not_set "k2";
      set ~key:"k2" "vvv";
      contains ~key:"k2" "vvv";
      set ~key:"k2" "uuu";
      contains ~key:"k" "V";
      contains ~key:"k2" "uuu";
      unset "k2";
      is_not_set "k2";
    ])
    >>= function
    | `Done -> return ()
    | `Not_done -> Test.fail "seq 4 not done"; return ()
  end
  >>= fun () ->
  (* Transations that fail hard: *)
  let test_with_debug_artificial_failure name f =
    DB.Debug.(global_debug := f "k2");
    begin
      Lwt.catch (fun () ->
          DB.act db2 DB.(seq [
              set ~key:"k2" "rrr";
              set ~key:"k2" "uuu";
              unset "k2";
            ])
          >>< function
          | _ -> Test.fail (sprintf "seq %s not exn" name); return ())
        (fun e -> return ())
    end
    >>= fun () ->
    DB.Debug.(global_debug := No);
    (* We should be like end of seq 6 *)
    begin DB.act db2 DB.(seq [
        is_not_set "k2";
        set ~collection:"c3" ~key:"k3" "uuu";
        unset ~collection:"c3" "k3";
        unset ~collection:"c3" "k3";
      ])
      >>= function
      | `Done -> return ()
      | `Not_done -> Test.fail (sprintf "seq %s+1 not done" name); return ()
    end
  in
  test_with_debug_artificial_failure "After_write"
    (fun k -> DB.Debug.After_write k)
  >>= fun () ->
  test_with_debug_artificial_failure "After_git_add"
    (fun k -> DB.Debug.After_git_add k)
  >>= fun () ->
  test_with_debug_artificial_failure "After_git_rm"
    (fun k -> DB.Debug.After_git_rm k)
  >>= fun () ->
  let check_collection collection result =
    let check r =
      let sort = List.sort ~cmp:String.compare in
      sort r = sort result in
    DB.get_all db2 ~collection
    >>= function
    | r when check r -> return ()
    | other ->
      Test.fail (sprintf "Collection test: in %S  \nexpecting [%s]  \ngot [%s]"
                   collection (String.concat ~sep:", " result)
                   (String.concat ~sep:", " other));
      return ()
  in
  check_collection "" [] >>= fun () ->
  check_collection "aslkdj" [] >>= fun () ->
  check_collection "c3" [] >>= fun () ->
  let collection = "c3" in
  DB.act db2 DB.(seq [set ~collection ~key:"k1" "v1";
                      set ~collection ~key:"k2" "v2"])
  >>= fun _ ->
  check_collection collection ["v1"; "v2"] >>= fun () ->
  let collection = "c4" in
  DB.act db2 DB.(seq [set ~collection ~key:"k1" "v1";
                      set ~collection ~key:"k2" "v2"])
  >>= fun _ ->
  check_collection collection ["v1"; "v2"] >>= fun () ->
  (* check_collection "c3" ["sld"] >>= fun () -> *)
  return ()

module Test_git_commands = struct
  let test_name = "Test_git_commands"
  module DB = Trakeva_git_commands
  let debug_mode v = Trakeva_git_commands.global_debug_level := (if v then 2 else 0)
end
module Test_sqlite = struct
  let test_name = "Test_sqlite" 
  module DB = Trakeva_sqlite
  let debug_mode v = Trakeva_sqlite.debug := v
end

let has_arg arlg possible =
  List.exists arlg ~f:(List.mem ~set:possible)

let find_arg argl ~name ~convert =
  List.find_map argl (fun arg ->
      match String.split ~on:(`Character '=') arg |> List.map ~f:String.strip with
      | n :: c :: [] when n = name -> convert c
      | _ -> None)

let () =
  let argl = Sys.argv |> Array.to_list |> List.tl_exn in
  Test.run_monad "git-db" git_db_test;

  Test.run_monad "basic with git"
    (basic_test (module Test_git_commands) (Test.new_tmp_dir ()));

  let sqlite_path = "/tmp/trakeva-sqlite-test" in
  ksprintf Sys.command "rm -fr %s" sqlite_path |> ignore;
  Test.run_monad "basic/sqlite" (basic_test (module Test_sqlite) sqlite_path);

  if has_arg argl ["bench"; "benchmarks"] then (
    ksprintf Sys.command "rm -fr %s" sqlite_path |> ignore;
    let collection = find_arg argl ~name:"collection" ~convert:Int.of_string in
    let big_string_kb = find_arg argl ~name:"kb" ~convert:Int.of_string in
    Test.run_monad "bench01" (fun () ->
        benchmark_01 ?collection ?big_string_kb
          (module Test_git_commands) (Test.new_tmp_dir ()) ()
        >>= fun (_, git_bench01) ->
        benchmark_01 ?collection ?big_string_kb
          (module Test_sqlite) sqlite_path ()
        >>= fun (_, sqlite_bench01) ->
        say "Bench01";
        List.iter git_bench01 ~f:(fun (n, f) ->
            say "git\t%s\t%F s" n f
          );
        List.iter sqlite_bench01 ~f:(fun (n, f) ->
            say "sqlite\t%s\t%F s" n f
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
