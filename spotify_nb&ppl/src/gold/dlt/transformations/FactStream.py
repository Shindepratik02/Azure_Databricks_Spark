import dlt

@dlt.table
def FactStreamstg():
    df=spark.readStream.table("spotify_catalog.silver.factstream")
    return df

dlt.create_streaming_table(name="factstream_gold")

dlt.create_auto_cdc_flow(
  target = "factstream_gold",
  source = "factstreamstg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1,
  track_history_except_column_list = None,
  name = None,
  once = False
)