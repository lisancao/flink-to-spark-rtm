"""Helper to set Real-Time Mode trigger on a DataStreamWriter."""

def with_real_time_trigger(dsw, checkpoint_interval="5 minutes"):
    jvm = dsw._spark._jvm
    jTrigger = jvm.org.apache.spark.sql.execution.streaming.RealTimeTrigger.apply(
        checkpoint_interval
    )
    dsw._jwrite = dsw._jwrite.trigger(jTrigger)
    return dsw
