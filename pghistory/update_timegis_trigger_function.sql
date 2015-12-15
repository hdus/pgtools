-- Function: update_timegis()

CREATE OR REPLACE FUNCTION update_timegis()
  RETURNS trigger AS
$BODY$
my ($sql_attrib,$sql_insert,$sql_values,$rv);

if ($_TD->{old}{archive_date} != undef) {
	return "SKIP"; #quietly disallow
}
else {
	if ($_TD->{new}{archive_date} == undef) {
		$sql_insert = "INSERT INTO ".$_TD->{table_schema}.".".$_TD->{table_name}." (";
		$sql_values = '';
		#prepare SQL statement to get all column_names and data types (udt_name)
		$sql_attrib = "SELECT column_name, udt_name FROM information_schema.columns WHERE table_schema = '".$_TD->{table_schema}."' AND table_name = '".$_TD->{table_name}."';";
		my $rv = spi_exec_query($sql_attrib);
		my $nrows = $rv->{processed};
		my $areaExists = false;
		my $lengthExists = false;
		my $change_affected_columns = "";
		#loop over all column names and concatenate SQL INSERT statement
		foreach my $rn (0 .. $nrows - 1) {
			my $row = $rv->{rows}[$rn];
			if ($row->{column_name} ne "gid") {
				if ($row->{column_name} eq "id") {
					$sql_insert .= "id,";
					$sql_values .= $_TD->{old}{gid}.",";
				}
				elsif ($row->{column_name} eq "archive_date") {
					$sql_insert .= "archive_date,";
					$sql_values .= "now(),";
				}
				else {
					$sql_insert .= "\"".$row->{column_name}."\",";
					if ($_TD->{old}{$row->{column_name}} eq undefined || $_TD->{old}{$row->{column_name}} eq "") {
						$sql_values .= "NULL,";
					}
					else {
						if ($row->{udt_name} eq "text" || $row->{udt_name} eq "varchar" || $row->{udt_name} eq "geometry" || $row->{udt_name} eq "timestamp" || $row->{udt_name} eq "change_type" || $row->{udt_name} eq "bool" || $row->{udt_name} eq "ltree") {
							$sql_values .= "'".$_TD->{old}{$row->{column_name}}."',";
							#now test if new value is different from old value for this column (non-text data types)
							if ($row->{column_name} ne "change_type" && $row->{column_name} ne "change_affected_columns" && $row->{column_name} ne "create_date" && $row->{column_name} ne "create_user" && $row->{column_name} ne "last_user") {
								if ($_TD->{new}{$row->{column_name}} ne $_TD->{old}{$row->{column_name}}) {
									$change_affected_columns .= $row->{column_name}.",";
								}
							}
						}
						else {
							$sql_values .= $_TD->{old}{$row->{column_name}}.",";
							if ($row->{column_name} eq "area") {
								$areaExists = true;
							}
							if ($row->{column_name} eq "length") {
								$lengthExists = true;
							}
							#now test if new value is different from old value for this column (non-text data types)
							if ($row->{column_name} ne "area" && $row->{column_name} ne "length") {
								if ($_TD->{new}{$row->{column_name}} != $_TD->{old}{$row->{column_name}}) {
									$change_affected_columns .= $row->{column_name}.",";
								}
							}
						}
					}
				}
			}
		}
		chop($sql_insert); #get rid of the last comma
		chop($sql_values); #get rid of the last comma
		chop($change_affected_columns); #get rid of the last comma
		$sql_insert .= ") VALUES (".$sql_values.");";
		#elog(NOTICE,"$sql_insert");
		$rv = spi_exec_query($sql_insert);
		$_TD->{new}{create_date} = "now()";
		# Remember who changed the record
		$rv = spi_exec_query("SELECT current_user;");
		$_TD->{new}{last_user} = $rv->{rows}[0]->{current_user};
		# set change_type
		$_TD->{new}{change_type} = 'update';
		# set change_affected_columns
		$_TD->{new}{change_affected_columns} = $change_affected_columns;
		# see if we need to update the area (if existing)
		if ($areaExists eq true) {
			if ($_TD->{new}{the_geom} ne $_TD->{old}{the_geom}) {
				$rv = spi_exec_query("SELECT ST_AREA('$_TD->{new}{the_geom}') AS polyarea");
				$_TD->{new}{area} = $rv->{rows}[0]->{polyarea};
			}
		}
		# see if we need to update the length (if existing)
		if ($lengthExists eq true) {
			if ($_TD->{new}{the_geom} ne $_TD->{old}{the_geom}) {
				$rv = spi_exec_query("SELECT ST_LENGTH('$_TD->{new}{the_geom}') AS linelength");
				$_TD->{new}{length} = $rv->{rows}[0]->{linelength};
			}
		}
	}
	return "MODIFY";
}
$BODY$
  LANGUAGE 'plperl' VOLATILE
  COST 100;
COMMENT ON FUNCTION update_timegis() IS 'This trigger checks gis tables and adds timestamps, user and change information.';
