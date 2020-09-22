from typing import Optional

import pyspark.sql.functions as F
from multipledispatch import Dispatcher

import ibis.common.exceptions as com
import ibis.expr.operations as ops
from ibis.expr.timecontext import TIME_COL
from ibis.expr.typing import TimeContext

compute_time_context = Dispatcher('compute_time_context')


@compute_time_context.register(ops.Node)
def compute_time_context_default(
    node, timecontext: Optional[TimeContext] = None, **kwargs
):
    return [timecontext for arg in node.inputs]


def filter_by_time_context(df, timecontext):
    """ Filter a Dataframe by given time context
    Parameters
    ----------
    df : Spark Dataframe
    timecontext: TimeContext

    Returns
    -------
    filtered Spark Dataframe
    """
    if not timecontext:
        return df

    begin, end = timecontext
    if TIME_COL in df.columns:
        return df.filter((F.col(TIME_COL) >= begin) & (F.col(TIME_COL) < end))
    else:
        raise com.TranslationError(
            "'time' column missing in Dataframe {}."
            "To use time context, a Timestamp column name 'time' must"
            "present in the table. ".format(df)
        )
