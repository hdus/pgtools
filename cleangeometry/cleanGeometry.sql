-- Function: cleangeometry(geometry)

-- DROP FUNCTION cleangeometry(geometry);

CREATE OR REPLACE FUNCTION cleangeometry(geom geometry)
  RETURNS geometry AS
$BODY$DECLARE
  inGeom ALIAS for $1;
  outGeom geometry;
  tmpLinestring geometry;

Begin
  
  outGeom := NULL;

-- Only process if geometry is not valid, 
-- otherwise put out without change
  IF NOT st_isSimple(inGeom) THEN

-- Clean Process for Polygon 
    IF (GeometryType(inGeom) = 'POLYGON' OR GeometryType(inGeom) = 'MULTIPOLYGON') THEN

    
-- create nodes at all self-intersecting lines by union the polygon boundaries
-- with the startingpoint of the boundary.  
      tmpLinestring := st_union(st_multi(st_boundary(inGeom)),st_pointn(st_boundary(inGeom),1));
      outGeom = st_buildarea(tmpLinestring);      
      IF (GeometryType(inGeom) = 'MULTIPOLYGON') THEN      
        RETURN st_multi(outGeom);
      ELSE
        RETURN outGeom;
      END IF;


------------------------------------------------------------------------------
-- Clean Process for LINESTRINGS, self-intersecting parts of linestrings 
-- will be divided into multiparts of the mentioned linestring 
------------------------------------------------------------------------------
    ELSIF (GeometryType(inGeom) = 'LINESTRING') THEN
    
-- create nodes at all self-intersecting lines by union the linestrings
-- with the startingpoint of the linestring.  
      outGeom := st_union(st_multi(inGeom),st_pointn(inGeom,1));
      RETURN outGeom;
    ELSIF (GeometryType(inGeom) = 'MULTILINESTRING') THEN 
      outGeom := st_multi(st_union(inGeom,st_pointN(st_geometryN(inGeom, 1), 1)));
      RETURN outGeom;
    ELSE 
      RAISE NOTICE 'The input type % is not supported',GeometryType(inGeom);
      RETURN inGeom;
    END IF;	  
  ELSE    
    RETURN inGeom;
  END IF;

End;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


