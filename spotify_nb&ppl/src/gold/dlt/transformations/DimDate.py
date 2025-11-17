import dlt

@dlt.table
def DimDatestg():
    df=spark.readStream.table("spotify_catalog.silver.dimdate")
    return df

dlt.create_streaming_table(name="dimdate_gold")

dlt.create_auto_cdc_flow(
  target = "dimdate_gold",
  source = "dimdatestg",
  keys = ["date_key"],
  sequence_by = "date",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)