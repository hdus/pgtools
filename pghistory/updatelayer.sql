/***************************************************************************
   Function: updatelayer("varchar", "varchar")
   Funktion zum inkrementellen Update von Geometrietabellen
  -------------------------------------------------------------------
    Date                 : 14. January 2003
    Copyright            : (C) 2003 by Dr. Horst Duester
    email                : horst dot duester at kappasys dot ch
  ***************************************************************************
  *                                                                         *
  *   This program is free software; you can redistribute it and/or modify  *
  *   it under the terms of the GNU General Public License as published by  *
  *   the Free Software Foundation; either version 2 of the License, or     *
  *   (at your option) any later version.                                   *
  *                                                                         *
  ***************************************************************************/

CREATE OR REPLACE FUNCTION updatelayer("varchar", "varchar")
  RETURNS bool AS
$BODY$
DECLARE
  in_new_layer ALIAS FOR $1;
  in_old_layer ALIAS FOR $2;
  tablecatalog TEXT;
  new_layer TEXT;
  old_layer TEXT;
  old_schema TEXT;
  new_schema TEXT;
  old_pkey_rec RECORD;
  new_pkey_rec RECORD;
  old_pkey TEXT;
  new_pkey TEXT;		
  att_check RECORD;
  attributes RECORD;
  new_attributes RECORD;		
  old_attributes RECORD;
  qry_update TEXT;
  qry_insert TEXT;
  qry_isSimple TEXT;		
  qry_tmp TEXT;
  fields TEXT;
  function TEXT;
  old_fields TEXT;
  new_fields TEXT;
  ins_fields TEXT;
  heute TEXT;
  n_arch_field TEXT;
  arch_fields TEXT;
  where_fields TEXT;
  where_fields_ext TEXT;
  old_geo_rec RECORD;
  new_geo_rec RECORD;
  pos INTEGER;
  integer_var INTEGER;		
  gt TEXT;
  insub_query TEXT;
  
  
  BEGIN
    pos := strpos(in_old_layer,'.');
    if pos=0 then 
        old_schema := 'public';
  	old_layer := in_old_layer; 
    else 
  	old_schema = substr(in_old_layer,0,pos);
  	pos := pos + 1; 
        old_layer = substr(in_old_layer,pos);
    END IF;
  
    pos := strpos(in_new_layer,'.');
    if pos=0 then 
        new_schema := 'public';
  	new_layer := in_new_layer; 
    else 
        new_schema = substr(in_new_layer,0,pos);
  	pos := pos+1; 
  	new_layer = substr(in_new_layer,pos);
    END IF;
  
  
  -- Vorbelegen der Variablen
    fields := '';
    old_fields := '';
    function:='';		
    heute := now();
    qry_insert := '';
    qry_update := '';
    integer_var := 0;
    tablecatalog := 'sogis';
  
  
  -- Feststellen wie die Geometriespalte der Layer heisst bzw. ob der Layer in der Tabelle geometry_columns definiert ist
     select into old_geo_rec f_geometry_column, type as geom_type 
     from public.geometry_columns 
     where f_table_schema = old_schema 
       and f_table_name = old_layer;
  
     IF NOT FOUND THEN
       RAISE EXCEPTION 'Die Tabelle % ist nicht als Geo-Layer in der Tabelle geometry_columns registriert', old_layer;
       RETURN False;
     END IF;
  	  
     select into new_geo_rec f_geometry_column, type as geom_type 
     from public.geometry_columns 
     where f_table_schema = new_schema 
      and  f_table_name = new_layer;
  
     IF NOT FOUND THEN
        RAISE EXCEPTION 'Die Tabelle % ist nicht als Geo-Layer in der Tabelle geometry_columns registriert', new_layer;
        RETURN False;
     END IF;
  		
  
  -- Prüfen, ob der new_layer mindestens der Struktur des old_layer entspricht	
     select into att_check col.column_name
     from information_schema.columns as col
     where table_catalog = tablecatalog::name
       and table_schema = old_schema::name
       and table_name = old_layer::name
       and column_name not in ('archive','archive_date','new_date')
       and (position('nextval' in lower(column_default)) is NULL or position('nextval' in lower(column_default)) = 0)		
     except
     select col.column_name
     from information_schema.columns as col
     where table_catalog = tablecatalog::name
       and table_schema = new_schema::name
       and table_name = new_layer::name;
  			 
  			
    IF FOUND THEN
       RAISE EXCEPTION 'Die Tabelle % entspricht nicht der Tabelle %', new_layer, old_layer;
       RETURN False;
    END IF;
   
    n_arch_field := ' ';
    arch_fields:=' ';
    where_fields:=' ';
  		
  -- Prüfen, ob der new_layer das Attribut archive enthält
    FOR new_attributes in select column_name as att
        from information_schema.columns
        where table_catalog = tablecatalog::name
          and table_schema = new_schema::name
          and table_name = new_layer::name
  
    LOOP
        IF new_attributes.att = 'archive'::name then
           n_arch_field := 'and n.archive=0 ';
  	END IF;
    END LOOP;
  		
    arch_fields :=  n_arch_field||' and o.archive=0 ';

  		
  -- Prüfen ob und welche Spalte der Primarykey der Tabelle old_layer ist 
    select into old_pkey_rec col.column_name 
    from information_schema.table_constraints as key,
         information_schema.key_column_usage as col
    where key.table_catalog = tablecatalog::name
      and key.table_schema = old_schema::name
      and key.table_name = old_layer::name
      and key.constraint_type='PRIMARY KEY'
      and key.table_catalog = col.table_catalog
      and key.table_schema = col.table_schema
      and key.table_name = col.table_name;	
  
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Die Tabelle % hat keinen Primarykey', old_layer;
  		  RETURN False;
    END IF;			
  
  
  -- Prüfen ob und welche Spalte der Primarykey der Tabelle new_layer ist
    select into new_pkey_rec col.column_name 
    from information_schema.table_constraints as key,
         information_schema.key_column_usage as col
    where key.table_catalog = tablecatalog::name
      and key.table_schema = new_schema::name
      and key.table_name = new_layer::name
      and key.constraint_type='PRIMARY KEY'
      and key.table_catalog = col.table_catalog
      and key.table_schema = col.table_schema
      and key.table_name = col.table_name;	
  
    IF NOT FOUND THEN
       RAISE EXCEPTION 'Die Tabelle % hat keinen Primarykey', new_layer;
       RETURN False;
    END IF;			
  			
    where_fields_ext := ' ';		
    
    insub_query := 'select '||old_pkey_rec.column_name||' as id 
	             from '||old_schema||'.'||old_layer||' 
  	             where archive=0 
  		      and '||old_pkey_rec.column_name||' not in 
  			   (select o.'||old_pkey_rec.column_name||' 
  			    from '||new_schema::name||'.'||new_layer::name||' as n,'||old_schema||'.'||old_layer||' as o 
  			    where md5(n.'||new_geo_rec.f_geometry_column||')=md5(o.'||old_geo_rec.f_geometry_column||') 
  			      and o.'||old_geo_rec.f_geometry_column||' && n.'||new_geo_rec.f_geometry_column;	
  		
  -- Alle Sequenzen ermitteln und unberücksichtigt lassen		
    FOR attributes in select column_name as att, data_type as typ
                    		from information_schema.columns as col
                    		where table_catalog = tablecatalog::name
                    		  and table_schema = old_schema::name
                    			and table_name = old_layer::name
                    			and column_name not in (old_geo_rec.f_geometry_column::name,'archive','archive_date','new_date','av_date')
                          and (position('nextval' in lower(column_default)) is NULL or position('nextval' in lower(column_default)) = 0)		
    LOOP
  
    old_fields := old_fields ||','|| attributes.att;
  					 
  -- Eine Spalte vom Typ Bool darf nicht in die Coalesce-Funktion gesetzt werden					 
    IF old_pkey_rec.column_name <> attributes.att THEN
       IF attributes.typ = 'bool' THEN
           where_fields := where_fields ||' and n.'||attributes.att||'=o.'||attributes.att;					 
       ELSE
  	   where_fields := where_fields ||' and coalesce(n.'||attributes.att||'::text,'''')=coalesce(o.'||attributes.att||'::text,'''')';
       END IF;
     END IF;
  
    END LOOP;
  		
    where_fields := where_fields||' '||arch_fields;
    insub_query := insub_query||' '||where_fields||')';
  
  
  -- Vorbereiten der Update Funktion
    qry_update := 'update '||old_schema||'.'||old_layer||' set archive_date='||quote_literal(heute)||', archive=1 
      	           from ('||insub_query||') as foo 
  		   where foo.id = '||old_schema||'.'||old_layer||'.'||old_pkey_rec.column_name;
  									 
  
  
  -- Ausführen der Update Funktion 
--RAISE NOTICE '%',qry_update;
--RETURN false;											
  EXECUTE qry_update;
  GET DIAGNOSTICS integer_var = ROW_COUNT;
  RAISE NOTICE ' % Objekte wurden im Layer %.% archiviert',integer_var,old_schema,old_layer;
  
  -- Vorbereiten der Insert Funktion
  
  insub_query := 'select '||new_geo_rec.f_geometry_column||' as '||old_geo_rec.f_geometry_column||''||old_fields||' 
                  from '||new_schema||'.'||new_layer||' as n
  		  where   
  		  '||new_pkey_rec.column_name||' not in 
  		   (select n.'||new_pkey_rec.column_name||' 
  		    from '||new_schema::name||'.'||new_layer::name||' as n,'||old_schema||'.'||old_layer||' as o 
  		    where md5(n.'||new_geo_rec.f_geometry_column||')=md5(o.'||old_geo_rec.f_geometry_column||') 
  		      and o.'||old_geo_rec.f_geometry_column||' && n.'||new_geo_rec.f_geometry_column||' '||where_fields||') '||n_arch_field||'';
  														
  qry_insert := 'insert into '||old_schema||'.'||old_layer||' ('||old_geo_rec.f_geometry_column||''||old_fields||') '||insub_query;
  
--  RAISE NOTICE '%',qry_insert;
--  RAISE NOTICE '%',insub_query;											
  EXECUTE qry_insert;
  GET DIAGNOSTICS integer_var = ROW_COUNT;
  RAISE NOTICE ' % Objekte wurden in den Layer %.% neu eingefügt',integer_var,old_schema,old_layer;
  
  RETURN true;
END;
  $BODY$
  LANGUAGE 'plpgsql' VOLATILE;
COMMENT ON FUNCTION updatelayer("varchar", "varchar") IS 'Funktion zum inkrementellen Update der SO!GIS Geometrietabellen';
