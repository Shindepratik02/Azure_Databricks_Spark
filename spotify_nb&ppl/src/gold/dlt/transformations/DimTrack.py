import dlt

@dlt.table
def DimTrackstg():
    df=spark.readStream.table("spotify_catalog.silver.dimtrack")
    return df

dlt.create_streaming_table(name="dimtrack_gold")

dlt.create_auto_cdc_flow(
  target = "dimtrack_gold",
  source = "dimtrackstg",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)