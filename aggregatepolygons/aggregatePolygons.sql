-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- $Id: aggregatePolygons.sql 2011-11-15 10:30Z Dr. Horst Duester $
--
-- aggregatePolygons - Combines grouped polygons within a specified 
--                     distance to each other into new polygons for 
--                     generalitation purposes 
-- http://www.kappasys.ch
-- Copyright 2011 Dr. Horst Duester
-- Version 1.0
-- contact: horst dot duester at kappasys dot ch
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
-- This software is without any warrenty and you use it at your own risk
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


-- 1. Composite-Type für den State
DROP TYPE IF EXISTS aggregatepolygons_state CASCADE;

CREATE TYPE aggregatepolygons_state AS (
    geom geometry,
    dist double precision,
    ortho boolean
);

-- 2. State-Funktion
CREATE OR REPLACE FUNCTION _aggregatepolygons_state(
    state aggregatepolygons_state,
    new_geom geometry,
    dist double precision,
    ortho boolean
)
RETURNS aggregatepolygons_state AS
$$
DECLARE
    theUnion geometry;
BEGIN
    IF state IS NULL OR state.geom IS NULL OR ST_IsEmpty(state.geom) THEN
        RETURN (new_geom, dist, ortho);
    END IF;

    theUnion := ST_Union(state.geom, new_geom);

    -- GeometryCollection → nur Polygone extrahieren
    IF GeometryType(theUnion) = 'GEOMETRYCOLLECTION' THEN
        theUnion := ST_CollectionExtract(theUnion, 3);
    END IF;

    RETURN (theUnion, state.dist, state.ortho);
END;
$$
LANGUAGE plpgsql
VOLATILE;

-- 3. Final-Funktion mit optimiertem Splitting
CREATE OR REPLACE FUNCTION _aggregatepolygons_final(
    state aggregatepolygons_state
)
RETURNS geometry AS
$$
DECLARE
    invDist double precision;
    geom_to_buffer geometry := state.geom;
    buffered geometry;
    single_geom geometry;
BEGIN
    IF state IS NULL OR state.geom IS NULL OR ST_IsEmpty(state.geom) THEN
        RETURN NULL;
    END IF;

    invDist := -state.dist;

    -- Nur MultiPolygons splitten, die mehr als 1 Polygon enthalten
    IF GeometryType(geom_to_buffer) = 'MULTIPOLYGON' THEN
        IF ST_NumGeometries(geom_to_buffer) > 1 THEN
            geom_to_buffer := (
                SELECT ST_Union(d.geom)
                FROM ST_Dump(geom_to_buffer) AS d
            );
        END IF;
    END IF;

    -- Puffer-Logik
    IF state.ortho THEN
        buffered := ST_Buffer(
                        ST_Buffer(geom_to_buffer, state.dist, 'join=mitre mitre_limit=2.5'),
                        invDist, 'join=mitre mitre_limit=2.5'
                    );
    ELSE
        buffered := ST_Buffer(geom_to_buffer, state.dist);
        buffered := ST_Buffer(buffered, invDist);
    END IF;

    -- Dump + Union → immer eine Geometry zurück
    RETURN (
        SELECT ST_Union(d.geom)
        FROM ST_Dump(buffered) AS d
    );
END;
$$
LANGUAGE plpgsql
VOLATILE;

-- 4. Aggregate erstellen
DROP AGGREGATE IF EXISTS aggregatepolygons(geometry, double precision, boolean);

CREATE AGGREGATE aggregatepolygons(
    geometry,
    double precision,
    boolean
)
(
    SFUNC = _aggregatepolygons_state,
    STYPE = aggregatepolygons_state,
    FINALFUNC = _aggregatepolygons_final
);

-- 5. Rechte vergeben
GRANT EXECUTE ON FUNCTION _aggregatepolygons_state(aggregatepolygons_state, geometry, double precision, boolean) TO public;
GRANT EXECUTE ON FUNCTION _aggregatepolygons_final(aggregatepolygons_state) TO public;

