SELECT
    track_id      AS song_id,
    name          AS song_name,
    album_id      AS song_album_id,
    media_type_id AS song_media_type_id,
    genre_id      AS song_genre_id,
    composer      AS song_composer,
    milliseconds  AS song_milliseconds,
    bytes         AS song_bytes,
    unit_price    AS song_unit_price
FROM stg_album_songs