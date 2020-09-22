import pandas as pd
import pandas.util.testing as tm
import pytest

import ibis

pytest.importorskip('pyspark')
pytestmark = pytest.mark.pyspark


def test_table_with_timecontext(client):
    table = client.table('time_indexed_table')
    context = (pd.Timestamp('20170102'), pd.Timestamp('20170103'))
    result = table.compile(timecontext=context).toPandas()
    expected = table.compile().toPandas()
    expected = expected[expected.time.between(*context)]
    tm.assert_frame_equal(result, expected)


def test_basic_window(client):
    table = client.table('time_indexed_table')
    context = (
        pd.Timestamp('20170102 07:00:00', tz='UTC'),
        pd.Timestamp('20170103', tz='UTC'),
    )
    window1 = ibis.trailing_window(
        preceding=ibis.interval(hours=1), order_by='time', group_by='key'
    )
    window2 = ibis.trailing_window(
        preceding=ibis.interval(hours=2), order_by='time', group_by='key'
    )
    result = table.mutate(
        mean_1h=table['value'].mean().over(window1),
        mean_2h=table['value'].mean().over(window2),
    ).compile(timecontext=context)
    result_pd = result.toPandas()

    df = table.compile().toPandas()
    expected_win_1 = (
        df.set_index('time')
        .groupby('key')
        .value.rolling('1h', closed='both')
        .mean()
        .rename('mean_1h')
    )
    expected_win_2 = (
        df.set_index('time')
        .groupby('key')
        .value.rolling('2h', closed='both')
        .mean()
        .rename('mean_2h')
    )
    df = df.set_index('time')
    df = df.assign(
        mean_1h=expected_win_1.sort_index(level=['time', 'key']).reset_index(
            level='key', drop=True
        )
    )
    df = df.assign(
        mean_2h=expected_win_2.sort_index(level=['time', 'key']).reset_index(
            level='key', drop=True
        )
    )
    df = df.reset_index()
    expected = df[
        df.time.between(*(t.tz_convert(None) for t in context))
    ].reset_index(drop=True)
    tm.assert_frame_equal(result_pd, expected)


def test_cumulative_window(client):
    # table = client.table('time_indexed_table')
    # context = (
    #     pd.Timestamp('20170102 06:00:00', tz='UTC'),
    #     pd.Timestamp('20170103', tz='UTC'),
    # )
    # window = ibis.cumulative_window(order_by='time', group_by='key')
    # result = table.mutate(
    #     count_cum=table['value'].count().over(window)
    # ).compile(timecontext=context)
    # result_pd = result.toPandas()
    pass


def test_complex_window(client):
    pass
