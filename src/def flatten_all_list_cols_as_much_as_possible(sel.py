	def flatten_all_list_cols_as_much_as_possible(self, polars_df, hard_stop=False, force_strings=False, just_these_columns=None,
		force_index=None):
		"""
		Flatten list columns as much as possible. If a column is just a bunch of one-element lists, for
		instance, then just take the 0th value of that list and make a column that isn't a list.

		If force_strings, any remaining columns that are still lists are forced into strings.
		"""
		# Do not run check index first, as it will break when this is run right after run-to-sample conversion
		if force_index is None:
			index_column = self.get_index_column(polars_df)
		else:
			index_column = force_index

		null_counts_before = polars_df.filter(pl.col(col).null_count() > 0 for col in polars_df.columns)
		if null_counts_before.shape[0] == 0:
			self.logging.debug("Dataframe already seems to have no nulls")
		else:
			self.logging.debug("Dataframe has some nulls")
			self.logging.debug(null_counts_before)

		self.logging.debug("Recursively unnesting lists...")
		polars_df = self.flatten_nested_list_cols(polars_df)
		self.logging.debug("Unnested all list columns. Index seems okay.")

		null_counts_after = polars_df.filter(pl.col(col).null_count() > 0 for col in polars_df.columns)
		if null_counts_after.shape[0] == 0:
			self.logging.debug("After recursively unnesting lists, dataframe seems to have no nulls")
		else:
			self.logging.debug("After recursively unnesting lists, dataframe has some nulls")
			self.logging.debug(null_counts_after)

		what_was_done = []

		if just_these_columns is None:
			col_dtype = polars_df.schema
		else:
			col_dtype = dict()
			for col in just_these_columns:
				assert col in polars_df
				dtype = polars_df.schema[col]
				col_dtype[col] = dtype
		
		for col, datatype in col_dtype.items():
			#self.logging.debug(f"Evaulating on {col} of type {datatype}") # summary table at the end should suffice
			
			if col in drop_zone.silly_columns:
				polars_df.drop(col)
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': pl.Null, 'result': 'dropped'})
				continue
			
			if datatype == pl.List and datatype.inner != datetime.datetime:

				try:
					polars_df = polars_df.with_columns(pl.col(col).list.drop_nulls())
				except Exception as e:
					self.logging.error(f"Tried to drop_nulls() from supposed list column {col} but caught {e}!")
					self.logging.error(f"{col} has type {datatype} but is acting like it isn't a list -- is it full of nulls?")
					self.logging.error(polars_df.select(col))
					exit(1)

				if col in kolumns.equivalence['run_index'] and index_column in kolumns.equivalence['sample_index']:
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (runs in samp-indexed df)'})
					continue
				
				elif polars_df[col].drop_nulls().shape[0] == 0:
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (empty/nulls)'})
					continue

				elif col in kolumns.special_taxonomic_handling:
					# First attempt to flatten ALL taxoncore stuff (yes, this will get repeated per col in kolumns.special_taxonomic_handling, too bad)
					for kolumn in kolumns.special_taxonomic_handling:
						if kolumn in polars_df.columns and polars_df.schema[kolumn] == pl.List:
							polars_df = polars_df.with_columns(pl.col(kolumn).list.unique())
							dataframe_height = polars_df.shape[1]
							polars_df = self.drop_nulls_from_possible_list_column(polars_df, kolumn)
							current_dataframe_height = polars_df.shape[1]
							assert dataframe_height == current_dataframe_height

							polars_df = self.coerce_to_not_list_if_possible(polars_df, kolumn, index_column, prefix_arrow=True)
					if polars_df.schema[col] == pl.List: # since it might not be after coerce_to_not_list_if_possible()
						#long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(['sample_index', 'clade', 'organism', 'lineage', 'strain'])
						long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(['sample_index', 'clade', 'organism', 'lineage']) # TODO: BAD WORKAROUND
						if len(long_boi) > 0:
							# TODO: more rules could be added, and this is a too TB specific, but for my purposes it's okay for now
							if col == 'organism' and polars_df.schema['organism'] == pl.List:
								# check lineage column first for consistency
								# TODO: these polars expressions are hilariously ineffecient but I want them explict for the time being
								if polars_df.schema['lineage'] == pl.List:
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L1')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L2')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L3')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L4')).list.all())).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L5')).list.all())).then(pl.lit(["Mycobacterium africanum"])).otherwise(pl.col("clade")).alias('organism'))
									polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L6')).list.all())).then(pl.lit(["Mycobacterium africanum"])).otherwise(pl.col("clade")).alias('organism'))
								polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() == 2).and_(pl.col('organism').list.contains("Mycobacterium tuberculosis complex sp."))).then(pl.lit(["Mycobacterium tuberculosis complex sp."])).otherwise(pl.col("organism")).alias("organism"))
								polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() == 2).and_(pl.col('organism').list.contains("Mycobacterium tuberculosis"))).then(pl.lit(["Mycobacterium tuberculosis complex sp."])).otherwise(pl.col("organism")).alias("organism"))
								# unnecessary
								#elif polars_df.schema['lineage'] == pl.Utf8:
								#	polars_df = polars_df.with_columns(pl.when((pl.col('organism').list.len() > 1).and_(pl.col('lineage').str.starts_with('L')).and_(~pl.col('lineage').str.starts_with('L5')).and_(~pl.col('lineage').str.starts_with('L6')).and_(~pl.col('lineage').str.starts_with('La'))).then(pl.lit(["Mycobacterium tuberculosis"])).otherwise(pl.col("organism")).alias('organism'))
							
							elif col == 'clade' and polars_df.schema['clade'] == pl.List:
								polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('clade').list.contains('MTBC')).and_(~pl.col('clade').list.contains('NTM'))).then(pl.lit(["MTBC"])).otherwise(pl.col("clade")).alias('clade'))
								
								if polars_df.schema['lineage'] == pl.List:
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L1')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L2')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L3')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L4')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L5')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L6')).list.all())).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))
								elif polars_df.schema['lineage'] == pl.Utf8:
									polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1).and_(pl.col('lineage').str.starts_with('L')).and_(~pl.col('lineage').str.starts_with('La'))).then(pl.lit(["tuberculosis: human-adapted"])).otherwise(pl.col("clade")).alias('clade'))

								# We'll treat every remaining conflict as tuberculosis
								# TODO: this is probably not how we should be handling this, but we need to delist this somehow and it works for my dataset
								polars_df = polars_df.with_columns(pl.when((pl.col('clade').list.len() > 1)).then(['tuberculosis: unclassified']).otherwise(pl.col("clade")).alias('clade'))
							
							elif col == 'lineage' and polars_df.schema['lineage'] == pl.List:
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L1')).list.all())).then(pl.lit(["L1"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L2')).list.all())).then(pl.lit(["L2"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L3')).list.all())).then(pl.lit(["L3"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L4')).list.all())).then(pl.lit(["L4"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L5')).list.all())).then(pl.lit(["L5"])).otherwise(pl.col("lineage")).alias('lineage'))
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1).and_(pl.col('lineage').list.eval(pl.element().str.starts_with('L6')).list.all())).then(pl.lit(["L6"])).otherwise(pl.col("lineage")).alias('lineage'))

								# We'll treat every remaining conflict as invalid and null it 
								polars_df = polars_df.with_columns(pl.when((pl.col('lineage').list.len() > 1)).then(None).otherwise(pl.col("lineage")).alias('lineage'))
							
							#long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(['sample_index', 'clade', 'organism', 'lineage', 'strain'])
							long_boi = polars_df.filter(pl.col(col).list.len() > 1).select(['sample_index', 'clade', 'organism', 'lineage']) # TODO BAD WORKAROUND
							print(f"After delongating {col}...")
							print(long_boi)
							polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
						else:
							self.logging.debug(f"Taxoncore column {col} will not be adjusted further")
				
				elif col in kolumns.list_to_float_sum:
					# TODO: use logger adaptors instead of this print cringe
					print(f"{col}\n-->[kolumns.list_to_float_sum]") if self.logging.getEffectiveLevel() == 10 else None
					if datatype.inner == pl.String:
						print(f"-->Inner type is string, casting to pl.Int32 first") if self.logging.getEffectiveLevel() == 10 else None
						polars_df = polars_df.with_columns(
							pl.col(col).list.eval(
								pl.when(pl.element().is_not_null())
								.then(pl.element().cast(pl.Int32))
								.otherwise(None)
							).alias(f"{col}_sum")
						)
					else:
						polars_df = polars_df.with_columns(pl.col(col).list.sum().alias(f"{col}_sum"))
					polars_df = polars_df.drop(col)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[f"{col}_sum"], 'result': 'namechange + summed'})
					continue
				
				elif col in kolumns.list_to_list_silent:
					print(f"{col}\n-->[kolumns.list_to_list_silent]") if self.logging.getEffectiveLevel() == 10 else None
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': '.'})
					continue

				elif col in kolumns.list_to_null:
					print(f"{col}\n-->[kolumns.list_to_null]") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = polars_df.with_columns([
						pl.when(pl.col(col).list.len() <= 1).then(pl.col(col)).otherwise(None).alias(col)
					])
					print(f"-->Set null in conflicts, now trying to delist") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'null conflicts'})
					continue
				
				elif col in kolumns.list_to_set_uniq: 
					print(f"{col}\n-->[kolumns.list_to_set_uniq]") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					print("-->Used uniq, now trying to delist") if self.logging.getEffectiveLevel() == 10 else None
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink'})
					continue
					
				elif col in kolumns.list_fallback_or_null:
					# If this had happened during a merge of two dataframes, we would be falling back on one df or the other. But here, we
					# don't know what value to fall back upon, so it's better to just null this stuff.
					self.logging.warning(f"{col}\n-->[kolumns.list_fallback_or_null] Expected {col} to only have one non-null per sample, but that's not the case. Will null those bits.")
					polars_df = polars_df.with_columns(pl.col(col).list.unique())
					polars_df = polars_df.with_columns([
						pl.when(pl.col(col).list.len() <= 1).then(pl.col(col)).otherwise(None).alias(col)
					])
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					#assert len(self.get_rows_where_list_col_more_than_one_value(polars_df, col, False)) == 0 # beware: https://github.com/pola-rs/polars/issues/19987
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set-and-shrink (!!!WARNING!!)'})
					if hard_stop:
						exit(1)
					else:
						continue
				else:
					self.logging.warning(f"{col}-->Not sure how to handle, will treat it as a set")
					polars_df = polars_df.with_columns(pl.col(col).list.unique().alias(f"{col}"))
					polars_df = self.coerce_to_not_list_if_possible(polars_df, col, index_column, prefix_arrow=True)
					what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'set (no rules)'})
					continue

			elif datatype == pl.List and datatype.inner == datetime.date:
				self.logging.warning(f"{col} is a list of datetimes. Datetimes break typical handling of lists, so this column will be left alone.")
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': 'skipped (date.date)'})
			
			else:
				what_was_done.append({'column': col, 'intype': datatype, 'outtype': polars_df.schema[col], 'result': '-'})
		
		if force_strings:
			if just_these_columns is None:
				polars_df = self.stringify_all_list_columns(polars_df)
			else:
				for column in just_these_columns:
					polars_df = self.stringify_one_list_column(polars_df, column)
		
		report = pl.DataFrame(what_was_done)
		if self.logging.getEffectiveLevel() <= 10:
			NeighLib.super_print_pl(report, "Finished flattening list columns. Results:")
		self.logging.debug("Returning flattened dataframe")
		return polars_df