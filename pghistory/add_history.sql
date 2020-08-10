-- Function: add_history(text, text, text, text, boolean)

CREATE OR REPLACE FUNCTION add_history(text, text, text, text, boolean)
  RETURNS boolean AS
$BODY$
my ($sql,$sql_attrib,$schema,$schemav,$table,$view,$sql_insert,$sql_values,$rv,$result,%columns);
$schema = $_[0];
$table = $_[1];
$schemav = $_[2];
$view = $_[3];
$createGeomColumnForView = $_[4];

#indicates whether function is successful or not
$result = true;

#see if schema exists
$sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '".$schema."';";
my $rv = spi_exec_query($sql);
my $nrows = $rv->{processed};

if ($nrows != 1) {
	elog(ERROR,"\nschema '$schema' does not exist!\n");
	$result = false;
}

#see if table exists
$sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '".$schema."' AND table_name = '".$table."';";
my $rv = spi_exec_query($sql);
my $nrows = $rv->{processed};

if ($nrows != 1) {
	elog(ERROR,"\nError: table '$table' does not exist in schema '$schema'\n");
	$result = false;
}

if ($result) {
	#prepare SQL statement to get all column_names and data types (udt_name)
	$sql_attrib = "SELECT column_name, udt_name FROM information_schema.columns WHERE table_schema = '".$schema."' AND table_name = '".$table."';";
	my $rv = spi_exec_query($sql_attrib);
	my $nrows = $rv->{processed};
	my @cols; #column/attribute names
	my @geom_columns;

	foreach my $rn (0 .. $nrows - 1) {
		my $row = $rv->{rows}[$rn];
		$columns{$row->{column_name}} = $row->{udt_name};
		push(@cols,$row->{column_name});
		if ($row->{udt_name} eq "geometry") {
			push(@geom_columns,$row->{column_name});
		}
		#elog(NOTICE,"$row->{column_name}: $row->{udt_name}\n");
	}

	#check if column gid with type integer or int4 is available
	if (exists $columns{'gid'}) {
		unless ($columns{'gid'} == 'int4' || $columns{'gid'} == 'integer') {
			elog(NOTICE,"\ncolumn 'gid' is not of datatype int4 or integer!\n");
			$result = false;
		}
	}
	else {
		elog(ERROR,"\nError: column 'gid' does not exist!\n");
		$result = false;
	}
	#check if column with name id and type integer exists
	unless (exists $columns{'id'}) {
		elog(NOTICE,"\ncreating column 'id' with datatype 'integer'\n");
		$sql = "ALTER TABLE ".$schema.".".$table." ADD COLUMN id integer;";
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nColumn id was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of column id failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		unless ($columns{'id'} == 'int4' || $columns{'id'} == 'integer') {
			elog(ERROR,"\nError: column 'id' already exists, but with wrong data type: $columns{'id'}.\n");
			$result = false;
		}
	}
	#check if column with name create_date exists
	unless (exists $columns{'create_date'}) {
		elog(NOTICE,"\ncreating column 'create_date' with datatype 'timestamp without time zone'\n");
		$sql = "ALTER TABLE ".$schema.".".$table." ADD COLUMN create_date timestamp without time zone;";
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nColumn create_date was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of column create_date failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		unless ($columns{'create_date'} == 'timestamp') {
			elog(ERROR,"\nError: column 'create_date' already exists, but with wrong data type: $columns{'create_date'}.\n");
			$result = false;
		}
	}
	#check if column with name archive_date exists
	unless (exists $columns{'archive_date'}) {
		elog(NOTICE,"\ncreating column 'archive_date' with datatype 'timestamp without time zone'\n");
		$sql = "ALTER TABLE ".$schema.".".$table." ADD COLUMN archive_date timestamp without time zone;";
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nColumn archive_date was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of column archive_date failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		unless ($columns{'archive_date'} == 'timestamp') {
			elog(ERROR,"\nError: column 'archive_date' already exists, but with wrong data type: $columns{'archive_date'}.\n");
			$result = false;
		}
	}
	#check if column with name create_user exists
	unless (exists $columns{'create_user'}) {
		elog(NOTICE,"\ncreating column 'create_user' with datatype 'text'\n");
		$sql = "ALTER TABLE ".$schema.".".$table." ADD COLUMN create_user text;";
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nColumn create_user was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of column create_user failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		unless ($columns{'create_user'} == 'text') {
			elog(ERROR,"\nError: column 'create_user' already exists, but with wrong data type: $columns{'create_user'}.\n");
			$result = false;
		}
	}
	#check if column with name last_user exists
	unless (exists $columns{'last_user'}) {
		elog(NOTICE,"\ncreating column 'last_user' with datatype 'text'\n");
		$sql = "ALTER TABLE ".$schema.".".$table." ADD COLUMN last_user text;";
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nColumn last_user was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of column last_user failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		unless ($columns{'last_user'} == 'text') {
			elog(ERROR,"\nError: column 'last_user' already exists, but with wrong data type: $columns{'last_user'}.\n");
			$result = false;
		}
	}
	#check if column with name change_type exists
	unless (exists $columns{'change_type'}) {
		elog(NOTICE,"\ncreating column 'change_type' with datatype 'change_type'\n");
		$sql = "ALTER TABLE ".$schema.".".$table." ADD COLUMN change_type change_type;";
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nColumn change_type was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of column change_type failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		unless ($columns{'change_type'} == 'change_type') {
			elog(ERROR,"\nError: column 'change_type' already exists, but with wrong data type: $columns{'change_type'}.\n");
			$result = false;
		}
	}
	#check if column with name change_affected_columns exists
	unless (exists $columns{'change_affected_columns'}) {
		elog(NOTICE,"\ncreating column 'change_affected_columns' with datatype 'text'\n");
		$sql = "ALTER TABLE ".$schema.".".$table." ADD COLUMN change_affected_columns text;";
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nColumn 'change_affected_columns' was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of column 'change_affected_columns' failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		unless ($columns{'change_affected_columns'} == 'text') {
			elog(ERROR,"\nError: column 'change_affected_columns' already exists, but with wrong data type: $columns{'change_affected_columns'}.\n");
			$result = false;
		}
	}
	
	#check if insert trigger is already available
	$sql = "SELECT * FROM information_schema.triggers WHERE event_object_schema = '".$schema."' AND event_object_table = '".$table."' AND trigger_name = 'insert_".$table."';";
	my $rv = spi_exec_query($sql);
	my $nrows = $rv->{processed};

	if ($nrows != 1) {
		$sql = "CREATE TRIGGER insert_".$table." BEFORE INSERT ON ".$schema.".".$table." FOR EACH ROW EXECUTE PROCEDURE insert_timegis();";
		elog(NOTICE,"\ncreating insert trigger 'insert_".$table."'\n");
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nTrigger 'insert_".$table."' was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of Trigger 'insert_".$table."' failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		elog(NOTICE,"\nInsert trigger for table '$table' already exists ...\n");
	}
	
	#check if update trigger is already available
	$sql = "SELECT * FROM information_schema.triggers WHERE event_object_schema = '".$schema."' AND event_object_table = '".$table."' AND trigger_name = 'update_".$table."';";
	my $rv = spi_exec_query($sql);
	my $nrows = $rv->{processed};

	if ($nrows != 1) {
		$sql = "CREATE TRIGGER update_".$table." BEFORE UPDATE ON ".$schema.".".$table." FOR EACH ROW EXECUTE PROCEDURE update_timegis();";
		elog(NOTICE,"\ncreating update trigger 'update_".$table."'\n");
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nTrigger 'update_".$table."' was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of Trigger 'update_".$table."' failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		elog(NOTICE,"\nUpdate trigger for table '$table' already exists ...\n");
	}

	#check if dele rule for original table exists
	$sql = "SELECT * FROM pg_rules WHERE schemaname = '".$schema."' AND tablename = '".$table."' AND rulename = '".$table."_del';";
	my $rv = spi_exec_query($sql);
	my $nrows = $rv->{processed};

	if ($nrows != 1) {
		$sql = "CREATE OR REPLACE RULE ".$table."_del AS ON DELETE TO ".$schema.".".$table." DO INSTEAD  UPDATE ".$schema.".".$table." SET archive_date = now(), last_user = current_user, change_type = 'delete' WHERE ".$table.".gid = old.gid AND ".$table.".archive_date IS NULL;";
		elog(NOTICE,"\ncreating delete rule '".$table."_del' \n");
		elog(NOTICE,"\n".$sql."\n");
		my $rv = spi_exec_query($sql);
		if ($rv->{status} eq "SPI_OK_UTILITY") {
			elog(NOTICE,"\nDELETE RULE '".$table."_del' was successfully created ...\n");
		}
		else {
			elog(ERROR,"\nError: creation of delete rule '".$table."_del' failed. Status=".$rv->{status}."\n");
		}
	}
	else {
		elog(NOTICE,"\nDelete rule for table '$table' already exists ...\n");
	}

	#handling view testing and creation
	if ($schemav && $view  && $schemav ne "none" && $view ne "none") {
		#test if view already exists
		$sql = "SELECT * from information_schema.views WHERE table_schema = '".$schemav."' AND table_name = '".$view."';";
		my $rv = spi_exec_query($sql);
		my $nrows = $rv->{processed};

		if ($nrows != 1) {
			elog(NOTICE,"\nCreating View with name '".$view."' \n");
			$sql = "CREATE OR REPLACE VIEW ".$schemav.".".$view." AS SELECT gid, id, ";

			#concatenate sql with all column names
			#make sure gid and id is on the front of the column names in the view
			foreach my $col (@cols) {
				if ($col !~ /^gid$|^id$|^create_date$|^archive_date$|^create_user$|^last_user$|^change_type$|^change_affected_columns$/) {
					$sql .= $col.", ";
				}
			}
			$sql .= "create_date, archive_date, create_user, last_user, change_type, change_affected_columns FROM ".$schema.".".$table." WHERE archive_date IS NULL;";
			elog(NOTICE,"\nsql=".$sql."\n");
			my $rv = spi_exec_query($sql);
			if ($rv->{status} eq "SPI_OK_UTILITY") {
				elog(NOTICE,"\nView '".$schemav.".".$view."' was successfully created ...\n");
			}
			else {
				elog(ERROR,"\nError: View '".$schemav.".".$view."' could not be created ...\n");
			}
		}
		else {
			elog(NOTICE,"\nView with name '".$view."' already present in schema '".$schemav."'.\n");
		}

		#create delete rule
		$sql = "SELECT * FROM pg_rules WHERE schemaname = '".$schemav."' AND tablename = '".$view."' AND rulename = '_delete';";
		my $rv = spi_exec_query($sql);
		my $nrows = $rv->{processed};

		if ($nrows != 1) {
			$sql = "CREATE OR REPLACE RULE _delete AS ON DELETE TO ".$schemav.".".$view." DO INSTEAD  DELETE FROM ".$schema.".".$table." WHERE ".$table.".gid = old.gid AND ".$table.".archive_date IS NULL;";
			elog(NOTICE,"\ncreating delete rule on view '".$schemav.".".$view."' ... \n");
			elog(NOTICE,"\n".$sql."\n");
			my $rv = spi_exec_query($sql);
			if ($rv->{status} eq "SPI_OK_UTILITY") {
				elog(NOTICE,"\nDELETE RULE on view '".$schemav.".".$view."' was successfully created ...\n");
			}
			else {
				elog(ERROR,"\nError: creation of delete rule on view '".$schemav.".".$view."' failed. Status=".$rv->{status}."\n");
			}
		}
		else {
			elog(NOTICE,"\nDelete rule on view '".$schemav.".".$view."' already exists ...\n");
		}

		#create insert rule
		$sql = "SELECT * FROM pg_rules WHERE schemaname = '".$schemav."' AND tablename = '".$view."' AND rulename = '_insert';";
		my $rv = spi_exec_query($sql);
		my $nrows = $rv->{processed};

		if ($nrows != 1) {
			$sql = "CREATE OR REPLACE RULE _insert AS ON INSERT TO ".$schemav.".".$view." DO INSTEAD  INSERT INTO ".$schema.".".$table." (";
			foreach my $col (@cols) {
				if ($col !~ /^gid$|^id$|^create_date$|^archive_date$|^create_user$|^last_user$|^change_type$|^change_affected_columns$/) {
					$sql .= $col.", ";
				}
			}
			#removing last space and comma
			chop($sql);
			chop($sql);
			$sql .= ") VALUES (";
			foreach my $col (@cols) {
				if ($col !~ /^gid$|^id$|^create_date$|^archive_date$|^create_user$|^last_user$|^change_type$|^change_affected_columns$/) {
					$sql .= "new.".$col.", ";
				}
			}
			#removing last space and comma
			chop($sql);
			chop($sql);
			$sql .= ");";
			elog(NOTICE,"\ncreating insert rule on view '".$schemav.".".$view."' ... \n");
			elog(NOTICE,"\n".$sql."\n");
			my $rv = spi_exec_query($sql);
			if ($rv->{status} eq "SPI_OK_UTILITY") {
				elog(NOTICE,"\INSERT RULE on view '".$schemav.".".$view."' was successfully created ...\n");
			}
			else {
				elog(ERROR,"\nError: creation of insert rule on view '".$schemav.".".$view."' failed. Status=".$rv->{status}."\n");
			}
		}
		else {
			elog(NOTICE,"\Insert rule on view '".$schemav.".".$view."' already exists ...\n");
		}

		#create update rule
		$sql = "SELECT * FROM pg_rules WHERE schemaname = '".$schemav."' AND tablename = '".$view."' AND rulename = '_update';";
		my $rv = spi_exec_query($sql);
		my $nrows = $rv->{processed};

		if ($nrows != 1) {
			$sql = "CREATE OR REPLACE RULE _update AS ON UPDATE TO ".$schemav.".".$view." DO INSTEAD  UPDATE ".$schema.".".$table." SET ";
			foreach my $col (@cols) {
				if ($col !~ /^gid$|^id$|^create_date$|^archive_date$|^create_user$|^last_user$|^change_type$|^change_affected_columns$/) {
					$sql .= $col."=new.".$col.", ";
				}
			}
			#removing last space and comma
			chop($sql);
			chop($sql);
			$sql .= " WHERE ".$table.".gid = new.gid;";
			elog(NOTICE,"\ncreating update rule on view '".$schemav.".".$view."' ... \n");
			elog(NOTICE,"\n".$sql."\n");
			my $rv = spi_exec_query($sql);
			if ($rv->{status} eq "SPI_OK_UTILITY") {
				elog(NOTICE,"\UPDATE RULE on view '".$schemav.".".$view."' was successfully created ...\n");
			}
			else {
				elog(ERROR,"\nError: creation of update rule on view '".$schemav.".".$view."' failed. Status=".$rv->{status}."\n");
			}
		}
		else {
			elog(NOTICE,"\nUpdate rule on view '".$schemav.".".$view."' already exists ...\n");
		}

		#handle geometry_columns entries for the newly created view
		if ($createGeomColumnForView eq "t") {
			foreach my $geom_col (@geom_columns) {
				# see if a geometry_columns entry already exists
				$sql = "SELECT oid FROM public.geometry_columns WHERE f_table_schema = '".$schemav."' AND f_table_name = '".$view."' AND f_geometry_column = '".$geom_col."';";
				my $rv = spi_exec_query($sql);
				my $nrows = $rv->{processed};
				if ($nrows == 0) {
					#query geometry and srid attributes from the original table
					my $sql1 = "SELECT DISTINCT ST_GeometryType(".$geom_col.") AS geom_type, ST_SrID(".$geom_col.") AS srid, ST_CoordDim(".$geom_col.") AS coord_dim FROM ".$schema.".".$table.";";
					my $rv1 = spi_exec_query($sql1);
					my $row = $rv1->{rows}[0];
					# see if an entry exists
					my $sql2 = "INSERT INTO public.geometry_columns (f_table_schema,f_table_name,f_geometry_column,coord_dimension,srid,type) VALUES ('".$schemav."','".$view."','".$geom_col."',".$row->{coord_dim}.",".$row->{srid}.",'".$row->{geom_type}."');";
					elog(NOTICE,"\ncreating a geometry_columns entry for view '".$schemav.".".$view."' and geometry_column '".$geom_col."'\n");
					elog(NOTICE,"\n".$sql."\n");
				}
				else {
					elog(NOTICE,"\nA geometry_columns entry for view '".$schemav.".".$view."' and geometry_column '".$geom_col."' already exists ...\n");
				}
			}
		}
	}
	else {
		elog(NOTICE,"\nNo Views and Rules had been created ...\n");
	}

	#create indizes
	#first select all indizes
	#$sql = "SELECT oid FROM public.geometry_columns WHERE f_table_schema = '".$schemav."' AND f_table_name = '".$view."' AND f_geometry_column = '".$geom_col."';";
}

if ($result eq false) {
	elog(NOTICE,"command failed due to errors ...\n\n");
}

return $result;
$BODY$
  LANGUAGE 'plperl' VOLATILE
  COST 100;
