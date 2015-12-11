-module(lager).

-export([
  error/2, warning/2, info/2, debug/2
]).


error(Format,Args) ->
  'Elixir.Logger':bare_log(error,io_lib:format(Format,Args),[]).

warning(Format,Args) ->
  'Elixir.Logger':bare_log(warn,io_lib:format(Format,Args),[]).

info(Format,Args) ->
  'Elixir.Logger':bare_log(info,io_lib:format(Format,Args),[]).

debug(_Format,_Args) -> ok.

%debug(Format,Args) ->
%  'Elixir.Logger':log(debug,io_lib:format(Format,Args),[]).
