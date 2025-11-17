import dlt

@dlt.table
def DimUserstg():
    df=spark.readStream.table("spotify_catalog.silver.dimuser")
    return df

dlt.create_streaming_table(name="dimuser_gold")

dlt.create_auto_cdc_flow(
  target = "dimuser_gold",
  source = "dimuserstg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)