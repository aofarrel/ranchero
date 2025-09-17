import subprocess
import shutil
import json
import re
import polars as pl
from tqdm import tqdm
from .statics import kolumns

# https://peps.python.org/pep-0661/
_DEFAULT_TO_CONFIGURATION = object()

class Query:

	def __init__(self, configuration, naylib):
		if configuration is None:
			raise ValueError("No configuration was passed to Query class. Ranchero is designed to be initialized with a configuration.")
		else:
			self.cfg = configuration
			self.logging = self.cfg.logger
			self.NeighLib = naylib

	def _default_fallback(self, cfg_var, value):
		if value == _DEFAULT_TO_CONFIGURATION:
			return self.cfg.get_config(cfg_var)
		return value

	def add_aws_size(self, polars_df: pl.DataFrame, s3_column: str, output_size_column="bytes", no_sign_request=True, continue_on_s3_error=False):
		"""
		Get size in bytes of files/paths in a dataframe and save that to a new column.

		# TODO: port over gs fixes, make a more general AWS metadata function
		"""
		assert s3_column in polars_df
		assert output_size_column not in polars_df.columns
		if shutil.which("aws") is None:
			self.logging.error("Couldn't find aws on $PATH")
			exit(1)
		joined_dfs = []
		for s3_uri in tqdm(polars_df[s3_column]):
			if continue_on_s3_error:
				try:
					temp_new_df = self._aws_s3_ls(s3_uri, no_sign_request=no_sign_request)
				except subprocess.CalledProcessError as e:
					self.logging.warning(f"Could not get metadata for {s3_uri}")
					continue
			else:
				temp_new_df = self._aws_s3_ls(s3_uri, no_sign_request=no_sign_request)
			joined_dfs.append(
				polars_df.filter(pl.col(s3_column) == pl.lit(s3_uri)).join(
					temp_new_df.rename({"s3_path_queried": s3_column, "bytes": output_size_column}), on=s3_column, how="left"
				)
			)
		return pl.concat(joined_dfs, how='vertical', rechunk=True)

	@staticmethod
	def _aws_s3_ls(uri: str, no_sign_request=True):
		"""
		Get some metadata (currently just size in bytes) of a particular file via aws s3 ls

		Currently doesn't support getting the datestamp/timestamp from last modified since
		I'm not sure the best way to handle AWS's formatting of it. Also, AWS handles timezone
		a little weirdly: https://github.com/aws/aws-cli/issues/5242

		For simplicity's sake I set this up to only match the first hit. So if 
		uri="s3://hprc-working/foo/bar/m84081_230623_212309_s3.hifi_reads.bc2015.bam"
		but there's also a 
		"s3://hprc-working/foo/bar/m84081_230623_212309_s3.hifi_reads.bc2015.bam.bai", a
		downstream file if you would, I'm skipping that bai file.
		"""
		if no_sign_request:
			aws_cmd = "aws", "s3", "ls", "--no-sign-request", uri
		else:
			aws_cmd = "aws", "s3", "ls", uri
		result = subprocess.run(aws_cmd, check=True, capture_output=True, text=True)
		rows = []
		for line in result.stdout.strip().splitlines():
			parts = line.strip().split(maxsplit=3)
			if len(parts) == 4:
				_, _, size_bytes, s3_path_queried_basename_only = parts # _, _, are datestamp and timestamp respectively
				# s3_path_queried_basename_only is also unused but if we ever want downstream files too it may be helpful
				rows.append([uri, int(size_bytes)])
				break
			else:
				self.logging.error("Subprocess returned 0, but could not parse output from `aws s3 ls` command")
				raise ValueError("Subprocess returned 0, but could not parse output from `aws s3 ls` command")
		return pl.DataFrame(rows, schema=["s3_path_queried", "bytes"], orient="row")


	def add_gcloud_metadata(self,
			polars_df: pl.DataFrame,
			gs_column: str,
			output_prefix=None,
			continue_on_gs_error=False,
			gs_metadata=_DEFAULT_TO_CONFIGURATION):
		"""
		Requires gcloud is on the path and, if necessary, authenticated.

		TODO: allow user to define output column names? maybe via config?
		"""
		return_fields = self._default_fallback("gs_metadata", gs_metadata)
		assert not any(f"{output_prefix}{column}" in return_fields for column in polars_df.columns)
		if shutil.which("gcloud") is None:
			self.logging.error("Couldn't find gcloud on $PATH")
			exit(1)
		
		# TODO: Creating all these little dataframes feels inefficient, is there another way of doing this? I
		# don't think we should modify polars_df while its being iterated but building to a copy might be more
		# efficient. (The true limiting reagent of this function is Google though so this might not really matter.)
		schema = polars_df.schema
		joined_dfs = []
		for row in tqdm(polars_df.iter_rows(named=True), total=polars_df.height):
			gs_uri = row[gs_column]
			if gs_uri is not None:
				if continue_on_gs_error:
					try:
						temp_new_df = self._gcloud_storage_objects_describe(gs_uri, gs_metadata=return_fields, output_prefix=output_prefix)
					except subprocess.CalledProcessError as e:
						self.logging.warning(f"Could not get metadata for {gs_uri}")
						oops_all_berries = dict.fromkeys([gs_column] + return_fields, pl.Utf8)
						oops_no_berries = dict.fromkeys([gs_column] + return_fields, None)
						#oops_no_berries = {k: [None] for k in [gs_column] + return_fields}
						temp_new_df = pl.DataFrame(oops_no_berries, schema_overrides=oops_all_berries)
						continue
				else:
					temp_new_df = self._gcloud_storage_objects_describe(gs_uri, gs_metadata=return_fields, output_prefix=output_prefix)
				self.NeighLib.dfprint(temp_new_df, loglevel=10)
				temp_new_df = temp_new_df.rename({"gs_uri": gs_column})
			else:
				# Even if gs_uri is pl.Null, we still need a dummy df of just this row so it can be included in the pl.concat,
				# or else we would basically drop any rows where gs_uri is null!
				self.logging.debug(f"Null value for {gs_uri}")
				oops_all_berries = dict.fromkeys([gs_column] + return_fields, pl.Utf8)
				oops_no_berries = dict.fromkeys([gs_column] + return_fields, None)
				#oops_no_berries = {k: [None] for k in [gs_column] + return_fields}
				temp_new_df = pl.DataFrame(oops_no_berries, schema_overrides=oops_all_berries)
				continue

			# join with original dataframe, whether or not we actually pinged Google this iteration
			joined_dfs.append(
				pl.DataFrame(row).cast(schema, strict=False) # necessary b/c polars may redefine the schema for a given row, which would break the final concat
				.join(
					temp_new_df, on=gs_column, how="left"
				)
			)
		return pl.concat(joined_dfs, how='vertical', rechunk=True)

	@staticmethod
	def _gcloud_storage_objects_describe(uri: str, gs_metadata: list, output_prefix: str):
		"""
		Grabs all metadata for a given gs URI, then narrows down based on gs_metadata list.
		"""
		if output_prefix is None:
			output_prefix = ''  # avoid f"{output_prefix}foo" becoming "Nonefoo"
		assert uri is not None
		assert uri != ""
		cmd = ["gcloud", "storage", "objects", "describe", uri, "--format", "json"]
		result = subprocess.run(cmd, capture_output=True, text=True, check=True)
		metadata = json.loads(result.stdout)
		output_df = pl.DataFrame({
			"gs_uri": uri,
			f"{output_prefix}bucket": metadata.get("bucket"),
			f"{output_prefix}content_type": metadata.get("content_type"),
			f"{output_prefix}crc32c_hash": metadata.get("crc32c_hash"),
			f"{output_prefix}creation_time": metadata.get("creation_time"),
			f"{output_prefix}etag": metadata.get("etag"),
			f"{output_prefix}generation": metadata.get("generation"),
			f"{output_prefix}md5_hash": metadata.get("md5_hash"),
			f"{output_prefix}metageneration": metadata.get("metageneration"),
			f"{output_prefix}name": metadata.get("name"),
			f"{output_prefix}size": int(metadata.get("size")),
			f"{output_prefix}storage_class": metadata.get("storage_class"),
			f"{output_prefix}storage_class_update_time": metadata.get("storage_class_update_time"),
			f"{output_prefix}storage_url": metadata.get("storage_url"),
			f"{output_prefix}update_time": metadata.get("update_time")
		}).with_columns(
			pl.col(f"{output_prefix}creation_time").str.strptime(pl.Datetime("us", "UTC"), strict=False)
		)
		actual_out_columns = ["gs_uri"] # we want this as the first column
		for out_column in gs_metadata:
			actual_out_columns.append(f"{output_prefix}{out_column}") # add everything else we want
		return output_df.select(actual_out_columns)


