-module(ebitmap).
-export([serialize/1]).
-export([deserialize/1]).
-export([create/0]).
-export([create/1]).
-export([add/2]).
-export([intersection/2]).
-export([union/2]).
-export([contains/2]).
-export([equals/2]).
-export([is_subset/2]).
-export([get_cardinality/1]).
-export([statistic/1]).
-export([f/1]).

-on_load(init/0).

init() ->
    SoName = filename:join(case code:priv_dir(?MODULE) of
                               {error, bad_name} ->
                                   Dir = code:which(?MODULE),
                                   filename:join([filename:dirname(Dir),"..", "priv"]);
                               Dir -> Dir
                           end, atom_to_list(?MODULE)),
    erlang:load_nif(SoName, 0).

intersection(_M1, _M2) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

union(_M1, _M2) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec serialize(reference()) -> {ok, binary()}.
serialize(_M) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec deserialize(reference()) -> {ok, binary()}.
deserialize(_M) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec create() -> {ok, reference()}.
create() ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec create(list()) -> {ok, reference()}.
create(_Vals) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

contains(_M1, _M2) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

equals(_M1, _M2) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

is_subset(_M1, _M2) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

add(_BM, _I) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

get_cardinality(_M) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec statistic(reference()) -> proplists:proplists().
statistic(_M) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

f(_BM) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).
