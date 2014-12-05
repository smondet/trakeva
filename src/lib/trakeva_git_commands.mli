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


(** {3 Implementation of the API} *)

include Trakeva_interface.KEY_VALUE_STORE

val global_debug_level: int ref
(** Debug-logging level used in the module (default: 4). *)

(** {3 Testing Help} *)

(** This module should be used only by the tests; with {!Debug.global_debug}
    one can inject a {i harder} failure (exception thrown)
    at a precise given point (see {!Debug.t}). *)
module Debug: sig

  type t =  No | After_write of string
         | After_git_add of string 
         | After_git_rm of string 

  val global_debug: t ref
end
