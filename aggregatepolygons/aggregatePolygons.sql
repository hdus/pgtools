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


-- Function: _aggregatepolygonsfunction(geometry, geometry, double precision, boolean)

CREATE OR REPLACE FUNCTION _aggregatepolygonsfunction(geometry, geometry, double precision, boolean)
  RETURNS geometry AS
$BODY$DECLARE
  invDist double precision;
  theUnionGeom geometry;
  theTmpTab Text;
  cmd Text;

Begin
  invDist := $3 * -1;

  IF not st_isEmpty($1) THEN
    theUnionGeom := st_union($1,$2);
    IF NOT st_isEmpty(theUnionGeom) THEN
      IF (GeometryType(theUnionGeom) = 'GEOMETRYCOLLECTION') THEN      
        theUnionGeom := st_geometryextract(theUnionGeom,3);     
      END IF;
      return cleanGeometry(theUnionGeom);        
    END IF;
  ELSE
    theTmpTab := 'tmp_'||round(extract(epoch from now()));
    BEGIN
      cmd := 'create temp table '||theTmpTab||' (dist double precision, ortho boolean);';
      execute cmd;
      cmd := 'insert into '||theTmpTab||' values ('||$3||','||$4||');'; 
      execute cmd;
      EXCEPTION
        WHEN duplicate_table THEN
      END;

    return $2;
  END IF;  
End;$BODY$
  LANGUAGE 'plpgsql' VOLATILE
  COST 100;

  
-- Function: _aggregatepolygonsfunction(geometry, geometry, double precision, boolean)


CREATE OR REPLACE FUNCTION _aggregatepolygonsfunctionbuffer(geometry)
  RETURNS geometry AS
$BODY$DECLARE
  invDist double precision;
  dist double precision;
  theRec record;
  theTmpTab text;

Begin
  theTmpTab := 'tmp_'||round(extract(epoch from now()));
  execute 'select * from '||theTmpTab into theRec;
  invDist := theRec.dist * -1;

  IF not st_isEmpty($1) THEN
    IF theRec.ortho THEN
--      RAISE NOTICE 'Ortho';
      return st_buffer(st_buffer($1,theRec.dist, 'join=mitre mitre_limit=2.5'),invDist, 'join=mitre mitre_limit=2.5');
    else
--      RAISE NOTICE 'non Ortho';
      return st_buffer(st_buffer($1,theRec.dist),invDist);
    END IF;
  END IF;  
End;$BODY$
  LANGUAGE 'plpgsql' VOLATILE
  COST 100;


GRANT EXECUTE ON FUNCTION _aggregatepolygonsfunction(geometry, geometry, double precision, boolean) TO public;
GRANT EXECUTE ON FUNCTION _aggregatepolygonsfunctionbuffer(geometry) TO public;

DROP AGGREGATE aggregatepolygons(geometry, double precision, boolean);
CREATE AGGREGATE aggregatepolygons(geometry, double precision, boolean)

(
  SFUNC=_aggregatepolygonsfunction,
  STYPE=geometry,
  FINALFUNC=_aggregatepolygonsfunctionbuffer
);
