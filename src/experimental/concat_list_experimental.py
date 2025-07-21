def concat_list_no_prop_nulls(polars_df, left_column, right_column):
	""" 

	UNFINISHED

	You have a dataframe you already merged on some column. You now have two columns,
	foo and foo_right, that you want to merge into a single list column. Ideally you'd use
	concat_list(), but for some reason that propagates nulls (ex: 1 + pl. Null = pl.Null).
	This function basically replaces concat_list() with a bunch of polars expressions that
	will avoid propagating nulls.
	Designed for:
	* left_column is list of str, right_column is str
	* left_column is str, right_column is list of str
	"""
	if polars_df.schema[left_column] == polars_df.schema[right_column]:
		raise ValueError(f"Not implemented on columns of the same datatype (both left and right are {polars_df.schema[left_column]})")
	
	# figure out which one is the list and which one is singular
	if polars_df.schema[right_column] == pl.List:
		list_column = right_column
		singular_column = left_column
	else:
		list_column = left_column
		singular_column = right_column
	assert polars_df.dtypes[singular_column] == polars_df.dtypes[list_column].inner

	# figure out the nullfill type
	# str: empty string
	# float: nan (untested but shouldn't propagate)
	# int: nan, but only after casting everything to float
	# boolean: .......uhhhhhhhhhhhhhhhhhhhhhhhhhhhh 2 idk
	match polars_df.schema[singular_column]:
		case pl.Utf8:
			empty = ""
		case pl.Float64:
			empty = np.nan

	# Wherever list is null, cast the singular column to list and use that value for new column.
	# Otherwise (ie when list column is not null), keep that value for the new column.
	polars_df = polars_df.with_columns(
		pl.when(pl.col(right_column).is_null())   
		.then(pl.col(left_column).cast(pl.List(str)))
		.otherwise(pl.col(right_column)) 
		.alias(f"{right_column}_nullfilled_with_left_col")
	)
	# Now that the right (list) column has had as many nulls removed as possible, we want to remove nulls 
	# from the left (str) column, because nulls in the left column would also cause issues with concat_list.
	polars_df = polars_df.with_columns(
		pl.when(pl.col(left_column).is_null())   
		.then(pl.lit(""))
		.otherwise(pl.col(left_column)) 
		.alias(f"{left_column}_nullfilled_with_empty_str"),
	)

	# Only now is polars_df safe to use pl.concat_list() without worrying about nulls propagating.
	polars_df = small_merge.with_columns(
		merged=pl.concat_list([f"{left_column}_nullfilled_with_empty_str", f"{right_column}_nullfilled_with_left_col"])
		.list.unique().list.drop_nulls()
	)

	# Remove empty strings from the list
	polars_df = polars_df.with_columns(
		pl.col("merged").list.eval(
			pl.element().filter(pl.element().str.len_chars() > 0)
		)
	)
	return polars_df.drop([f"{left_column}_nullfilled_with_empty_str", f"{right_column}_nullfilled_with_left_col", right_column, left_column])	