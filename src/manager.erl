%%%%  Copyright (c) 2009 Mark Fine <mark.fine@gmail.com>
%%%%
%%%%  Permission is hereby granted, free of charge, to any person
%%%%  obtaining a copy of this software and associated documentation
%%%%  files (the "Software"), to deal in the Software without
%%%%  restriction, including without limitation the rights to use,
%%%%  copy, modify, merge, publish, distribute, sublicense, and/or sell
%%%%  copies of the Software, and to permit persons to whom the
%%%%  Software is furnished to do so, subject to the following
%%%%  conditions:
%%%%
%%%%  The above copyright notice and this permission notice shall be
%%%%  included in all copies or substantial portions of the Software.
%%%%
%%%%  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%%%%  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%%%%  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%%%%  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%%%%  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%%%%  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%%%%  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%%%%  OTHER DEALINGS IN THE SOFTWARE.

%%%%  $Id: manager.erl,v 1.1.1.1 2009/05/06 07:56:42 mfine Exp $
%%%%
%%%%  This module is a re-purposed supervisor. It allows access to
%%%%  managed child processes by local names and allows children to
%%%%  be created dynamically. Otherwise, it provides similar interface
%%%%  and functionality as supervisor.

-module(manager).

-author("Mark Fine <mark.fine@gmail.com").

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

%% API
-export([start_link/2, start_link/3, start_child/2,
         terminate_child/2, which_child/2, which_children/1]).

-export([behaviour_info/1]).

-record(state, {children,
                pids,
                strategy,
                intensity,
                period,
                name,
                module,
                args,
                restarts = []}).

-record(child, {name, 
                pid = undefined,
                mfa,
                restart,
                shutdown}).

-define(is_running(Child), Child#child.pid =/= undefined).


%%% =======================================================================
%%%  API
%%% =======================================================================

behaviour_info(callbacks) ->
    [{init,1}];
behaviour_info(_Other) ->
    undefined.

start_link(Mod, Args) ->
    gen_server:start_link(?MODULE, {self, Mod, Args}, []).

start_link(Name, Mod, Args) ->
    gen_server:start_link(Name, ?MODULE, {Name, Mod, Args}, []).

start_child(Manager, ChildInfo) ->
    gen_server:call(Manager, {start_child, ChildInfo}).

terminate_child(Manager, Name) ->
    gen_server:call(Manager, {terminate_child, Name}).

which_child(Manager, Name) ->
    gen_server:call(Manager, {which_child, Name}).

which_children(Manager) ->
    gen_server:call(Manager, which_children).


%%% =======================================================================
%%%  gen_server callbacks
%%% =======================================================================

init({Name, Mod, Args}) ->
    process_flag(trap_exit, true),
    case Mod:init(Args) of
        {ok, {Info, ChildrenInfo}} ->
            case catch check_state(Info, Name, Mod, Args) of
                {ok, State} ->
                    case check_children(ChildrenInfo) of
                        {ok, Children} -> 
                            case start_children(Children, State#state.name) of
                                {ok, NChildren} ->
                                    add_children(NChildren, State);
                                Error ->
                                    {stop, {shutdown, Error}}
                            end;
                        Error ->
                            {stop, {children_info, Error}}
                    end;
                Error ->
                    {stop, {manager_info, Error}}
            end;
        ignore ->
            ignore;
        Error ->
            {stop, {bad_return, Error}}
    end.

terminate(_Reason, State) ->
    terminate_children(children_to_list(State), State#state.name),
    ok.

code_change(_OVsn, State, _Extra) ->
    case (State#state.module):init(State#state.args) of
        {ok, {Info, ChildrenInfo}} ->
            case catch check_state(Info, State) of
                {ok, NState} ->
                    case check_children(ChildrenInfo) of
                        {ok, Children} ->
                            update_children(Children, NState);
                        Error ->
                            {error, Error}
                    end;
                Error ->
                    {error, Error}
            end;
        ignore ->
            {ok, State};
        Error ->
            Error
    end.

handle_call({start_child, ChildInfo}, _From, State) ->
    case catch check_child(ChildInfo) of
        {ok, Child} ->
            case find_name(Child#child.name, State) of
                error ->
                    case start(Child, State#state.name) of
                        {ok, NChild} ->
                            {reply, {ok, NChild#child.pid}, add_child(NChild, State)};
                        {error, What} ->
                            {reply, {error, What}, State}
                    end;
                {ok, OChild} ->
                    {reply, {ok, OChild#child.pid}, State}
            end;
        What ->
            {reply, {error, What}, State}
    end;

handle_call({terminate_child, Name}, _From, State) ->
    case find_name(Name, State) of
        {ok, Child} ->
            abort(Child, State#state.name),
            {reply, {ok, Child#child.pid}, del_child(Child, State)};
        error ->
            {reply, {error, undefined}, State}
    end;

handle_call({which_child, Name}, _From, State) ->
    case find_name(Name, State) of
        {ok, Child} ->
            {reply, {value, Child#child.pid}, State};
        error ->
            {reply, undefined, State}
    end;

handle_call(which_children, _From, State) ->
    {reply, pids_to_list(State), State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    case find_pid(Pid, State) of
        {ok, Child} ->
            case restart(Child#child.restart, Reason, Child, State) of
                {ok, NState} ->
                    {noreply, NState};
                {shutdown, NState} ->
                    {stop, shutdown, NState}
            end;
        error ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.


%%% =======================================================================
%%%  Internal
%%% =======================================================================

%%% -----------------------------------------------------------------------
%%%  Manager info
%%% -----------------------------------------------------------------------
check_state({Strategy, Intensity, Period}, Name, Mod, Args) ->
    valid_strategy(Strategy),
    valid_intensity(Intensity),
    valid_period(Period),
    {ok, #state{strategy=Strategy,
                intensity=Intensity,
                period=Period,
                name=name(Name, Mod),
                module=Mod,
                args=Args}};
check_state(Info, _Name, _Mod, _Args) ->
    {invalid_manager, Info}.

check_state({Strategy, Intensity, Period}, State) ->
    valid_strategy(Strategy),
    valid_intensity(Intensity),
    valid_period(Period),
    {ok, State#state{strategy=Strategy,
                     intensity=Intensity,
                     period=Period}};
check_state(Info, _State) ->
    {invalid_manager, Info}.

valid_strategy(one_for_one) -> true;
valid_strategy(one_for_all) -> true;
valid_strategy(Strategy) -> throw({invalid_strategy, Strategy}).

valid_intensity(Intensity) when is_integer(Intensity), Intensity >= 0 -> true;
valid_intensity(Intensity) -> throw({invalid_intensity, Intensity}).

valid_period(Period) when is_integer(Period), Period > 0 -> true;
valid_period(Period) -> throw({invalid_period, Period}).

name(self, Mod) -> {self(), Mod};
name(Name, _Mod) -> Name.

%%% -----------------------------------------------------------------------
%%%  Child info
%%% -----------------------------------------------------------------------
check_children(ChildrenInfo) ->
    check_children(ChildrenInfo, []).

check_children([ChildInfo | T], Children) ->
    case catch check_child(ChildInfo) of
        {ok, Child} ->
            case lists:keysearch(Child#child.name, #child.name, Children) of
                {value, OChild} ->
                    {duplicate, OChild#child.name};
                false ->
                    check_children(T, [Child | Children])
            end;
        Error ->
            Error
    end;
check_children([], Children) ->
    {ok, Children}.

check_child({Name, Mfa, Restart, Shutdown}) ->
    valid_mfa(Mfa),
    valid_restart(Restart),
    valid_shutdown(Shutdown),
    {ok, #child{name=Name,
                mfa=Mfa,
                restart=Restart,
                shutdown=Shutdown}};
check_child(ChildInfo) ->
    {invalid_child, ChildInfo}.

valid_mfa({M, F, A}) when is_atom(M); is_tuple(M), is_atom(F), is_list(A) -> true;
valid_mfa(Mfa) -> throw({invalid_mfa, Mfa}).

valid_restart(permanent) -> true;
valid_restart(temporary) -> true;
valid_restart(transient) -> true;
valid_restart(Restart) -> throw({invalid_restart, Restart}).

valid_shutdown(Shutdown) when is_integer(Shutdown), Shutdown > 0 ->true;
valid_shutdown(infinity) -> true;
valid_shutdown(brutal_kill) -> true;
valid_shutdown(Shutdown) -> throw({invalid_shutdown, Shutdown}).

%%% -----------------------------------------------------------------------
%%%  start Child
%%% -----------------------------------------------------------------------
start_children(Children, Name) ->
    start_children(Children, [], Name).

start_children([Child | T], Children, Name) ->
    case start(Child, Name) of
        {ok, NChild} ->
            start_children(T, [NChild | Children], Name);
        Error ->
            terminate_children(Children, Name),
            Error
    end;
start_children([], Children, _Name) ->
    {ok, Children}.

start(Child, Name) ->
    #child{mfa={M, F, A}} = Child,
    case catch apply(M, F, A) of
        {ok, Pid} when is_pid(Pid) ->
            NChild = Child#child{pid=Pid},
            report_info(start, NChild, Name),
            {ok, NChild};
        ignore ->
            {ok, undefined};
        {error, What} ->
            report_error(start, What, Child, Name),
            {error, What};
        What ->
            report_error(start, What, Child, Name),
            {error, What}
    end.

%%% -----------------------------------------------------------------------
%%%  terminate Child
%%% -----------------------------------------------------------------------
terminate_children(Children, Name) ->
    terminate_children(Children, [], Name).

terminate_children([Child | T], Children, Name) ->
    NChild = abort(Child, Name),
    terminate_children(T, [NChild | Children], Name);
terminate_children([], Children, _Name) ->
    Children.

abort(Child, Name) when ?is_running(Child) ->
    report_info(terminate, Child, Name),
    case shutdown(Child#child.pid, Child#child.shutdown) of
        ok ->
            Child#child{pid=undefined};
        {error, What} ->
            report_error(terminate, What, Child, Name),
            Child#child{pid=undefined}
    end;
abort(Child, _Name) ->
    Child.

shutdown(Pid, brutal_kill) ->
    case monitor_child(Pid) of
	ok ->
	    exit(Pid, kill),
	    receive
		{'DOWN', _Ref, process, Pid, killed} ->
		    ok;
		{'DOWN', _Ref, process, Pid, Reason} ->
		    {error, Reason}
	    end;
	{error, Reason} ->      
	    {error, Reason}
    end;

shutdown(Pid, Time) ->
    case monitor_child(Pid) of
	ok ->
	    exit(Pid, shutdown),
	    receive 
		{'DOWN', _Ref, process, Pid, shutdown} ->
		    ok;
		{'DOWN', _Ref, process, Pid, Reason} ->
		    {error, Reason}
	    after Time ->
		    exit(Pid, kill),
		    receive
			{'DOWN', _Ref, process, Pid, Reason} ->
			    {error, Reason}
		    end
	    end;
	{error, Reason} ->      
	    {error, Reason}
    end.

monitor_child(Pid) ->
    erlang:monitor(process, Pid),
    unlink(Pid),
    receive
        {'EXIT', Pid, Reason} ->
            receive
                {'DOWN', _Ref, process, Pid, _Reason} ->
                    {error, Reason}
            end
    after 0 ->
            ok
    end.

%%% -----------------------------------------------------------------------
%%%  restart Child
%%% -----------------------------------------------------------------------
restart(permanent, Reason, Child, State) ->
    report_error(child_exit, Reason, Child, State#state.name),
    restart(Child, State);
restart(_Restart, normal, Child, State) ->
    {ok, del_child(Child, State)};
restart(_Restart, shutdown, Child, State) ->
    {ok, del_child(Child, State)};
restart(temporary, Reason, Child, State) ->
    report_error(child_exit, Reason, Child, State#state.name),
    {ok, del_child(Child, State)};
restart(transient, Reason, Child, State) ->
    report_error(child_exit, Reason, Child, State#state.name),
    restart(Child, State).

restart(Child, State) ->
    report_info(restart, Child, State#state.name),
    case check_restart(State) of
        {ok, NState} ->
            restart(NState#state.strategy, Child, NState);
        {terminate, NState} ->
            report_error(shutdown, max_restarts, Child, State#state.name),
            {shutdown, del_child(Child, NState)}
    end.

restart(one_for_one, Child, State) ->
    case start(Child, State#state.name) of
        {ok, NChild} ->
            {ok, add_child(NChild, del_child(Child, State))};
        {error, _What} ->
            restart(Child, State)
    end;
restart(one_for_all, Child, State) ->
    NState = add_child(Child#child{pid=undefined}, del_child(Child, State)),
    case start_children(
           terminate_children(children_to_list(NState), State#state.name), State#state.name) of
        {ok, NChildren} ->
            add_children(NChildren, NState);
        _Error ->
            restart(Child, NState)
    end.

check_restart(State) ->  
    Now = erlang:now(),
    Restarts = check_restart([Now | State#state.restarts], Now, State#state.period),
    NState = State#state{restarts=Restarts},
    case length(Restarts) of
	I when I  =< State#state.intensity ->
	    {ok, NState};
	_ ->
	    {terminate, NState}
    end.

check_restart([Restart | Restarts], Now, Period) ->
    case diff(Restart, Now) of
        T when T > Period ->
            [];
        _ ->
            [Restart | check_restart(Restarts, Now, Period)]
    end;
check_restart([], _Now, _Period) ->
    [].

diff({TimeM, TimeS, _}, {CurM, CurS, _}) when CurM > TimeM ->
    ((CurM - TimeM) * 1000000) + (CurS - TimeS);
diff({_, TimeS, _}, {_, CurS, _}) ->
    CurS - TimeS.

%%% -----------------------------------------------------------------------
%%%  Child helpers
%%% -----------------------------------------------------------------------
find_name(Name, State) ->
    dict:find(Name, State#state.children).

find_pid(Pid, State) ->
    case dict:find(Pid, State#state.pids) of
        {ok, Name} ->
            find_name(Name, State);
        error ->
            error
    end.

children(Children) ->
    dict:from_list(
      lists:map(fun(Child) -> {Child#child.name, Child} end, Children)).

pids(Children) ->
    dict:from_list(
      lists:map(fun(Child) -> {Child#child.pid, Child#child.name} end,
        lists:filter(fun(Child) -> ?is_running(Child) end, Children))).

pids_to_list(State) ->
    lists:map(fun({Pid, Name}) -> {Name, Pid} end, dict:to_list(State#state.pids)).

children_to_list(State) ->
    lists:map(fun({_Name, Child}) -> Child end, dict:to_list(State#state.children)).

add_children(Children, State) ->
    {ok, State#state{children=children(Children), pids=pids(Children)}}.

update_children(_Children, State) ->
    %% TODO -- update dicts
    {ok, State}.

add_child(Child, State) when ?is_running(Child) ->
    Children = dict:store(Child#child.name, Child, State#state.children),
    Pids = dict:store(Child#child.pid, Child#child.name, State#state.pids),
    State#state{children=Children, pids=Pids};
add_child(Child, State) ->
    Children = dict:store(Child#child.name, Child, State#state.children),
    State#state{children=Children}.

del_child(Child, State) ->
    Children = dict:erase(Child#child.name, State#state.children),
    Pids = dict:erase(Child#child.pid, State#state.pids),
    State#state{children=Children, pids=Pids}.

%%% -----------------------------------------------------------------------
%%%  reporting
%%% -----------------------------------------------------------------------
report_child(Child) ->
    {child,
     {pid, Child#child.pid},
     {name, Child#child.name},
     {mfa, Child#child.mfa},
     {restart, Child#child.restart},
     {shutdown, Child#child.shutdown}}.

report_info(Info, Child, Name) ->
    Msg = [{manager, Name},
           {info, Info},
           report_child(Child)],
    error_logger:info_report(progress, Msg).

report_error(Error, Reason, Child, Name) ->
    Msg = [{manager, Name},
           {error, Error},
           {reason, Reason},
           report_child(Child)],
    error_logger:error_report(manager_report, Msg).
            

