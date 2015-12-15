-- Function: insert_timegis()

CREATE OR REPLACE FUNCTION insert_timegis()
  RETURNS trigger AS
$BODY$
my ($sql,$rv, $rv1, $rv2, $affected_columns,$nrows);
if ($_TD->{new}{create_date} == undef) {
$_TD->{new}{create_date} = "now()";
$_TD->{new}{archive_date} = undef;
$_TD->{new}{id} = $_TD->{new}{gid};
$_TD->{new}{create_user} = "current_user()";
# Remember who inserted the record
$rv = spi_exec_query("SELECT current_user;");
$_TD->{new}{create_user} = $rv->{rows}[0]->{current_user};
$_TD->{new}{change_type} = "insert";
#prepare SQL statement to get all column_names and data types (udt_name)
$sql_attrib = "SELECT column_name FROM information_schema.columns WHERE table_schema = '".$_TD->{table_schema}."' AND table_name = '".$_TD->{table_name}."' AND column_name NOT IN ('gid','id','create_date','archive_date','create_user','last_user','change_type','change_affected_columns');";
$rv = spi_exec_query($sql_attrib);
$nrows = $rv->{processed};
#loop over all column names and concatenate SQL INSERT statement
$affected_columns = "";
foreach my $rn (0 .. $nrows - 1) {
	my $row = $rv->{rows}[$rn];
	if ($row->{column_name} eq "area" || $row->{column_name} eq "flaeche") {
		$rv1 = spi_exec_query("SELECT ST_AREA('$_TD->{new}{the_geom}') AS polyarea");
		$_TD->{new}{area} = $rv1->{rows}[0]->{polyarea};
	}
	elsif ($row->{column_name} eq "length" || $row->{column_name} eq "laenge") {
		$rv2 = spi_exec_query("SELECT ST_LENGTH('$_TD->{new}{the_geom}') AS linelength");
		$_TD->{new}{length} = $rv2->{rows}[0]->{linelength};
	}
	else {
		if ($_TD->{new}{$row->{column_name}}) {
			$affected_columns .= $row->{column_name}.",";
		}
	}
}
chop($affected_columns);
$_TD->{new}{change_affected_columns} = $affected_columns;

}
return "MODIFY";
$BODY$
  LANGUAGE 'plperl' VOLATILE
  COST 100;
COMMENT ON FUNCTION insert_timegis() IS 'This trigger checks gis tables and adds timestamps, user and change information.';
