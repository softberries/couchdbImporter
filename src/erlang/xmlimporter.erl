-module(xmlimporter).
-author("softwarepassion").

-import(queue,[in/1,out/1,new/0]).
-export([start/0]).
-include("src/couchdb/couch_db.hrl").

-define(ADMIN_USER_CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>]}}).

start() -> StartTime = now(),
  run("xmlimporter/dbdump_artistalbumtrack.0.290905586176.xml",0),
  Duration = timer:now_diff(now(), StartTime) / 1000000,
  io:format("Finished in: ~p~n",[Duration]),
  ok.

%% Read the file and execute the callback fun for each tag (start/end and character content)
run(File, Result) ->
  delete_db(<<"erlang_music">>),
  {ok, created} = create_db(<<"erlang_music">>),
  case file:read_file(xml(File)) of
    {ok, Bin} ->
      {ok,_,_} = erlsom:parse_sax(Bin, [], fun callback/2);
    Error ->
      Error
  end,
  Result.

%% Process tags and store the results inside the accumulator 'Acc'
callback(Event, Acc) -> processTag(Event,Acc).

%
%% Process START Tags ....
%
processTag({startElement,[],"location",[],[]},Acc) -> [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,
  {Art} = Artist,
  processStartTag(<<"location">>,[Path,AlbumNumber,TrackNumber,TagNumber,{Art++[{<<"location">>,{[]}}]}]);
processTag({startElement,[],"Albums",[],[]},Acc) -> [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,
  {Art} = Artist,
  processStartTag(<<"Albums">>,[Path,AlbumNumber,TrackNumber, TagNumber,{Art++[{<<"Albums">>,[]}]}]);

processTag({startElement,[],"album",[],[]},Acc) -> [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,
  {Key,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {Art} = Artist,
  processStartTag(<<"album">>,[Path,AlbumNumber+1,TrackNumber,TagNumber,{lists:keyreplace(Key,1,Art,{Key,Albums++[{[{<<"album">>,{[]}}]}]})}]);

processTag({startElement,[],"track",[],[]},Acc) -> [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,
  {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {KeyT,Tracks} = getByKeyFromAlbum(Artist,<<"Tracks">>,AlbumNumber),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  TracksNew = lists:keyreplace(KeyT,1,Album,{KeyT,Tracks++[{[{<<"track">>,{[]}}]}]}),
  AlbumsNew = replaceElement(Albums,AlbumNumber,{[{<<"album">>,{TracksNew}}]}),
  {Art} = Artist,
  NewArtist = lists:keyreplace(<<"Albums">>,1,Art,{<<"Albums">>,AlbumsNew}),
  processStartTag(<<"track">>,[Path,AlbumNumber,TrackNumber+1,TagNumber,{NewArtist}]);

processTag({startElement,[],"tag",[],[]},Acc) -> [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,
  {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {_,Tracks} = getByKeyFromAlbum(Artist,<<"Tracks">>,AlbumNumber),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  {[{_,{Track}}]} = lists:nth(TrackNumber,Tracks),
  {_,Tags} = lists:keyfind(<<"Tags">>, 1, Track),
  TrackNew = lists:keyreplace(<<"Tags">>,1,Track,{<<"Tags">>,Tags++[{[{<<"tag">>,{[]}}]}]}),
  TracksNew = replaceElement(Tracks,TrackNumber,{[{<<"track">>,{TrackNew}}]}),
  AlbumNew = lists:keyreplace(<<"Tracks">>,1,Album,{<<"Tracks">>,TracksNew}),
  AlbumsNew = replaceElement(Albums,AlbumNumber,{[{<<"album">>,{AlbumNew}}]}),
  {Art} = Artist,
  NewArtist = lists:keyreplace(<<"Albums">>,1,Art,{<<"Albums">>,AlbumsNew}),
  processStartTag(<<"tag">>,[Path,AlbumNumber,TrackNumber,TagNumber+1,{NewArtist}]);

processTag({startElement,[],"Tracks",[],[]},Acc) -> [Path,AlbumNumber,_,_,Artist] = Acc, processStartTag(<<"Tracks">>,addTracks(Path,AlbumNumber,0, 0,Artist));
processTag({startElement,[],"Tags",[],[]},Acc) -> [Path,AlbumNumber,TrackNumber,_,Artist] = Acc, processStartTag(<<"Tags">>,addTags(Path,AlbumNumber,TrackNumber, 0,Artist));

processTag({startElement,[],T,[],[]},Acc) ->
  case isValidTag(T) of
    true  -> processStartTag(list_to_binary(T),Acc);
    _Else -> Acc
  end;

%% End processing START Tags

%
%% Process END Tags
processTag({endElement,[],T,[]},Acc) ->
  case isValidTag(T) of
    true  -> processEndTag(list_to_binary(T),Acc);
    _Else -> Acc
  end;

processTag({characters,DataPlain},Acc) ->
  Data = convertToString(DataPlain),
  [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,
%  io:format("AlbumNr: ~p, TrackNr: ~p, TagNr: ~p, Path: ~p, Data: ~p, Artist: ~p~n",[AlbumNumber,TrackNumber,TagNumber,Path,Data,Artist]),
  case Path of
%Artist
    [<<"artist">>] -> Acc;
    [Property,<<"artist">>] -> setArtistProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property);
%Location
    [Property,<<"location">>,<<"artist">>] -> setLocationProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property);
%Albums
    [Property,<<"album">>,<<"Albums">>,<<"artist">>] -> setAlbumProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property);
%Tracks
    [Property,<<"track">>,<<"Tracks">>,<<"album">>,<<"Albums">>,<<"artist">>] -> setTrackProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property);
%Tags
    [Property,<<"tag">>,<<"Tags">>,<<"track">>,<<"Tracks">>,<<"album">>,<<"Albums">>,<<"artist">>] -> setTagProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property);
    _Else    -> Acc
  end;

processTag(_,ok) -> [];
processTag(_,Acc) -> Acc.

%% Allow all tags other than..
isValidTag(T) ->
  case lists:member(T,["JamendoData","Artists"]) of
    true -> false;
    false -> true
  end.
%
%% Add empty tracks structure for holding number of tracks
%
addTracks(Path,AlbumNumber,TrackNumber, TagNumber, Artist) ->
  {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  Replaced = replaceElement(Albums,AlbumNumber,{[{<<"album">>,{Album++[{<<"Tracks">>,[]}]}}]}),
  {Art} = Artist,
  NewArtist = lists:keyreplace(<<"Albums">>,1,Art,{<<"Albums">>,Replaced}),
  [Path,AlbumNumber,TrackNumber,TagNumber,{NewArtist}].
%
%% Add empty Tags structure for holding number of tags associated with a single track
%
addTags(Path,AlbumNumber,TrackNumber,TagNumber,Artist) ->
  {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {_,Tracks} = getByKeyFromAlbum(Artist,<<"Tracks">>,AlbumNumber),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  {[{_,{Track}}]} = getByKeyFromTracks(Tracks,TrackNumber),
  TracksNew = replaceElement(Tracks,TrackNumber,{[{<<"track">>,{Track++[{<<"Tags">>,[]}]}}]}),
  AlbumNew = lists:keyreplace(<<"Tracks">>,1,Album,{<<"Tracks">>,TracksNew}),
  AlbumsNew = replaceElement(Albums,AlbumNumber,{[{<<"album">>,{AlbumNew}}]}),
  {Art} = Artist,
  NewArtist = lists:keyreplace(<<"Albums">>,1,Art,{<<"Albums">>,AlbumsNew}),
  [Path,AlbumNumber,TrackNumber,TagNumber,{NewArtist}].

convertToString(DataPlain) ->
  try
    list_to_binary(DataPlain)
  catch
    _:_ -> []
  end.
%
%% Create new empty list while parsing new artist
processStartTag(<<"artist">>, _) -> Path = [], [[<<"artist">>|Path],0,0,0,{[]}];
%% Process all other tags using accumulator 'Acc'
processStartTag(T,Acc) ->
  [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,
  [[T|Path],AlbumNumber,TrackNumber,TagNumber,Artist].


%
%% Closing tags and resetting counters..
%
processEndTag(<<"artist">>,Acc) ->
  [_,_,_,_,ArtistData] = Acc,
  save_doc(<<"erlang_music">>,ArtistData),
  [];


processEndTag(<<"Albums">>,Acc) -> [Path,_,_,_,Artist] = Acc, [_|Queue] = Path, [ Queue, 0,0,0, Artist];
processEndTag(<<"Tracks">>,Acc) -> [Path,AlbumNumber,_,_,Artist] = Acc, [_|Queue] = Path, [ Queue, AlbumNumber,0,0, Artist];
processEndTag(<<"Tags">>,Acc) -> [Path,AlbumNumber,TrackNumber,_,Artist] = Acc, [_|Queue] = Path, [ Queue, AlbumNumber,TrackNumber,0, Artist];
processEndTag(<<"location">>,Acc) -> [Path,_,_,_,Artist] = Acc, [_|Queue] = Path, [Queue, 0,0,0, Artist];
processEndTag(_,Acc) -> [Path,AlbumNumber,TrackNumber,TagNumber,Artist] = Acc,[_|Queue] = Path, [Queue, AlbumNumber,TrackNumber,TagNumber, Artist].

setAlbumProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property) -> T = {Property,Data},
  {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  Replaced = replaceElement(Albums,AlbumNumber,{[{<<"album">>,{Album++[T]}}]}),
  {Art} = Artist,
  NewArtist = lists:keyreplace(<<"Albums">>,1,Art,{<<"Albums">>,Replaced}),
  [Path,AlbumNumber,TrackNumber,TagNumber,{NewArtist}].

setTrackProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property) -> T = {Property,Data},
  {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {_,Tracks} = getByKeyFromAlbum(Artist,<<"Tracks">>,AlbumNumber),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  {[{_,{Track}}]} = getByKeyFromTracks(Tracks,TrackNumber),
  TracksNew = replaceElement(Tracks,TrackNumber,{[{<<"track">>,{Track++[T]}}]}),
  AlbumNew = lists:keyreplace(<<"Tracks">>,1,Album,{<<"Tracks">>,TracksNew}),
  AlbumsNew = replaceElement(Albums,AlbumNumber,{[{<<"album">>,{AlbumNew}}]}),
  {Art} = Artist,
  NewArtist = lists:keyreplace(<<"Albums">>,1,Art,{<<"Albums">>,AlbumsNew}),
  [Path,AlbumNumber,TrackNumber,TagNumber,{NewArtist}].

setTagProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property) -> T = {Property, Data},
  {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {_,Tracks} = getByKeyFromAlbum(Artist,<<"Tracks">>,AlbumNumber),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  {[{_,{Track}}]} = getByKeyFromTracks(Tracks,TrackNumber),
  {_,Tags} = getByKeyFromTrack(Track,<<"Tags">>),
  {[{_,{Tag}}]} = lists:nth(TagNumber,Tags),
  TagsNew = replaceElement(Tags,TagNumber,{[{<<"tag">>,{Tag++[T]}}]}),
  TrackNew = lists:keyreplace(<<"Tags">>,1,Track,{<<"Tags">>,TagsNew}),
  TracksNew = replaceElement(Tracks,TrackNumber,{[{<<"track">>,{TrackNew}}]}),
  AlbumNew = lists:keyreplace(<<"Tracks">>,1,Album,{<<"Tracks">>,TracksNew}),
  AlbumsNew = replaceElement(Albums,AlbumNumber,{[{<<"album">>,{AlbumNew}}]}),
  {Art} = Artist,
  NewArtist = lists:keyreplace(<<"Albums">>,1,Art,{<<"Albums">>,AlbumsNew}),
  [Path,AlbumNumber,TrackNumber,TagNumber,{NewArtist}].

setLocationProperty(Path,Artist,Data,AlbumNumber,TrackNumber, TagNumber,Property) -> T = {Property,Data}, {Key,Loc} = getByKeyFromArtist(Artist,<<"location">>),
  {Location} = Loc,
  {Arts} = Artist,
  Art = lists:keyreplace(Key,1,Arts,{Key,{Location++[T]}}),
  [Path,AlbumNumber,TrackNumber,TagNumber,{Art}].

setArtistProperty(Path,Artist,Data,AlbumNumber,TrackNumber,TagNumber,Property) -> {Art} = Artist, [Path,AlbumNumber,TrackNumber,TagNumber,{Art++[{Property,Data}]}].

replaceElement(List,Position,Element) ->
  case Position of
    1 -> []++[Element];
    _Else -> lists:sublist(List,Position-1) ++ [Element]
  end.

%
%% We could live without the methods below:
%
getByKeyFromArtist(Artist,Key) -> {Art} = Artist, lists:keyfind(Key, 1, Art).

getByKeyFromAlbum(Artist,Key,AlbumNumber) -> {_,Albums} = getByKeyFromArtist(Artist,<<"Albums">>),
  {[{_,{Album}}]} = lists:nth(AlbumNumber,Albums),
  lists:keyfind(Key, 1, Album).

getByKeyFromTracks(Tracks,TrackNumber) -> lists:nth(TrackNumber,Tracks).

getByKeyFromTrack(Track,Key) -> lists:keyfind(Key,1,Track).

%% this is just to make it easier to test this little example
xml(File) -> filename:join([codeDir(), File]).
codeDir() -> filename:dirname(code:which(?MODULE)).

%% Taken from hovercraft library

open_db(DbName) ->
  couch_db:open(DbName, [?ADMIN_USER_CTX]).

create_db(DbName) ->
  create_db(DbName, []).

create_db(DbName, Options) ->
  case couch_server:create(DbName, Options) of
    {ok, Db} ->
      couch_db:close(Db),
      {ok, created};
    Error ->
      {error, Error}
  end.

delete_db(DbName) ->
  delete_db(DbName,  [?ADMIN_USER_CTX]).

delete_db(DbName, Options) ->
  case couch_server:delete(DbName, Options) of
    ok ->
      {ok, deleted};
    Error ->
      {error, Error}
  end.

save_doc(#db{}=Db, Doc) ->
  CouchDoc = ejson_to_couch_doc(Doc),
  {ok, Rev} = couch_db:update_doc(Db, CouchDoc, []),
  {ok, {[{id, CouchDoc#doc.id}, {rev, couch_doc:rev_to_str(Rev)}]}};

save_doc(DbName, Docs) ->
  {ok, Db} = open_db(DbName),
  save_doc(Db, Docs).

ejson_to_couch_doc({DocProps}) ->
  Doc = case proplists:get_value(<<"_id">>, DocProps) of
    undefined ->
      DocId = couch_uuids:new(),
      {[{<<"_id">>, DocId}|DocProps]};
    _DocId ->
      {DocProps}
  end,
  couch_doc:from_json_obj(Doc).
