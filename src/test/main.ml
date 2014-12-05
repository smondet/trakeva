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
    let db_file = Filename.concat (Sys.getcwd ()) "_tmp_test"  in
    begin System.Shell.do_or_fail (sprintf "rm -rf %s" db_file)
      >>< fun _ -> return ()
    end
    >>= fun () ->
    return db_file

end

let mini_db_test () =
  Lwt_main.run begin
    let module DB = Trakeva_git_commands in
    Test.new_tmp_dir ()
    >>= fun db_file ->
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
  end
  |> function
  | `Ok () -> ()
  | `Error e ->
    Test.fail "mini_db_test ends with error"






let () =
  let argl = Sys.argv |> Array.to_list |> List.tl_exn in
  mini_db_test ();
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
