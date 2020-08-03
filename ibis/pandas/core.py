"""The pandas backend is a departure from the typical ibis backend in that it
doesn't compile to anything, and the execution of the ibis expression
is under the purview of ibis itself rather than executing SQL on a server.

Design
------
The pandas backend uses a technique called `multiple dispatch
<https://en.wikipedia.org/wiki/Multiple_dispatch>`_, implemented in a
third-party open source library called `multipledispatch
<https://github.com/mrocklin/multipledispatch>`_.

Multiple dispatch is a generalization of standard single-dispatch runtime
polymorphism to multiple arguments.

Compilation
-----------
This is a no-op because we execute ibis expressions directly.

Execution
---------
Execution is divided into different dispatched functions, each arising from
a different use case.

A top level function `execute` exists to provide the API for executing an ibis
expression against in-memory data.

The general flow of execution is:

::
       If the current operation is in scope:
           return it
       Else:
           execute the arguments of the current node

       execute the current node with its executed arguments

Specifically, execute is comprised of a series of steps that happen at
different times during the loop.

1. ``compute_time_context``
First, at the beginning of the main execution loop, ``compute_time_context`` is
called. This function computes time contexts, and pass them to all children of
the current node. These time contexts could be used in later steps to get data.
This is essential for time series TableExpr, and related operations that adjust
time context, such as window, asof_join, etc.

By default, this function simply pass the unchanged time context to all
children nodes.


2. ``pre_execute``
------------------
Second, ``pre_execute`` is called.
This function serves a similar purpose to ``data_preload``, the key difference
being that ``pre_execute`` is called *every time* there's a call to execute.

By default this function does nothing.

3. ``execute_node``
-------------------

Then, when an expression is ready to be evaluated we call
:func:`~ibis.pandas.core.execute` on the expressions arguments and then
:func:`~ibis.pandas.dispatch.execute_node` on the expression with its
now-materialized arguments.

4. ``post_execute``
-------------------
The final step--``post_execute``--is called immediately after the previous call
to ``execute_node`` and takes the instance of the
:class:`~ibis.expr.operations.Node` just computed and the result of the
computation.

The purpose of this function is to allow additional computation to happen in
the context of the current level of the execution loop. You might be wondering
That may sound vague, so let's look at an example.

Let's say you want to take a three day rolling average, and you want to include
3 days of data prior to the first date of the input. You don't want to see that
data in the result for a few reasons, one of which is that it would break the
contract of window functions: given N rows of input there are N rows of output.

Defining a ``post_execute`` rule for :class:`~ibis.expr.operations.WindowOp`
allows you to encode such logic. One might want to implement this using
:class:`~ibis.expr.operations.ScalarParameter`, in which case the ``scope``
passed to ``post_execute`` would be the bound values passed in at the time the
``execute`` method was called.


Scope
-------------------
Scope is used across the execution phases, it iss a map that maps Ibis
operators to actual data. It is used to cache data for calculated ops. It is
an optimization to reused executed results.

With time context included, the key is op associated with each expression;
And scope value is another key-value map:
- value: pd.DataFrame or pd.Series that is the result of executing key op
- timecontext: of type TimeContext, the time context associated with the data
stored in value

Note that the idea is: data should be determined by both op and timecontext.
So the data structure above is conceptually same as making (op, timecontext)
as the key for scope. But that may increase the time complexity in querying,
so we make timecontext as another key of op. See following getting and setting
logic for details.

Set scope kv pair: before setting the value op in scope we need to perform the
following check first:

Test if op is in scope yet
- No, then put op in scope, set timecontext to be the current timecontext (None
if timecontext is not present), set value to be the DataFrame or Series of the
actual data.
- Yes, then get the timecontext stored in scope for op as old_timecontext, and
compare it with current timecontext:
If current timecontext is a subset of old_timecontext, that means we already
cached a larger range of data. Do nothing and we will trim data in later
execution process.
If current timecontext is a superset of old_timecontext, that means we need
to update cache. Set value to be the current data and set timecontext to be the
current timecontext for op.
If current timecontext is neither a subset nor a superset of old_timcontext,
but they overlap, or not overlap at all. For example this will happen when
there is a window that looks forward, over a window that looks back. So in this
case, we should not trust the data stored either, and go on to execute this
node. For simplicity, we update cache in this case as well.

See scope_item() for the structure of scope, and get_scope() for the details
about the implementaion.
"""

from __future__ import absolute_import

import datetime
import enum
import functools
import numbers
from typing import Optional

import numpy as np
import pandas as pd
import toolz
from multipledispatch import Dispatcher

import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.expr.window as win
import ibis.pandas.aggcontext as agg_ctx
from ibis.client import find_backends
from ibis.expr.typing import TimeContext
from ibis.pandas.dispatch import (
    execute_literal,
    execute_node,
    post_execute,
    pre_execute,
)
from ibis.pandas.trace import trace

integer_types = np.integer, int
floating_types = (numbers.Real,)
numeric_types = integer_types + floating_types
boolean_types = bool, np.bool_
fixed_width_types = numeric_types + boolean_types
date_types = (datetime.date,)
time_types = (datetime.time,)
timestamp_types = pd.Timestamp, datetime.datetime, np.datetime64
timedelta_types = pd.Timedelta, datetime.timedelta, np.timedelta64
temporal_types = date_types + time_types + timestamp_types + timedelta_types
scalar_types = fixed_width_types + temporal_types
simple_types = scalar_types + (str, type(None))


@functools.singledispatch
def is_computable_input(arg):
    """All inputs are not computable without a specific override."""
    return False


@is_computable_input.register(ibis.client.Client)
@is_computable_input.register(ir.Expr)
@is_computable_input.register(dt.DataType)
@is_computable_input.register(type(None))
@is_computable_input.register(win.Window)
@is_computable_input.register(tuple)
def is_computable_input_arg(arg):
    """Return whether `arg` is a valid computable argument."""
    return True


# Register is_computable_input for each scalar type (int, float, date, etc).
# We use consume here to avoid leaking the iteration variable into the module.
ibis.util.consume(
    is_computable_input.register(t)(is_computable_input_arg)
    for t in scalar_types
)


def execute_with_scope(
    expr,
    scope,
    timecontext: Optional[TimeContext] = None,
    aggcontext=None,
    clients=None,
    **kwargs,
):
    """Execute an expression `expr`, with data provided in `scope`.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        The expression to execute.
    scope : collections.Mapping
        A dictionary mapping :class:`~ibis.expr.operations.Node` subclass
        instances to concrete data such as a pandas DataFrame.
    timecontext : Optional[TimeContext]
        A tuple of (begin, end) that is passed from parent Node to children
        see [timecontext.py](ibis/pandas/execution/timecontext.py) for
        detailed usage for this time context.
    aggcontext : Optional[ibis.pandas.aggcontext.AggregationContext]

    Returns
    -------
    result : scalar, pd.Series, pd.DataFrame
    """
    op = expr.op()

    # Call pre_execute, to allow clients to intercept the expression before
    # computing anything *and* before associating leaf nodes with data. This
    # allows clients to provide their own data for each leaf.
    if clients is None:
        clients = list(find_backends(expr))

    if aggcontext is None:
        aggcontext = agg_ctx.Summarize()

    pre_executed_scope = pre_execute(
        op,
        *clients,
        scope=scope,
        timecontext=timecontext,
        aggcontext=aggcontext,
        **kwargs,
    )
    new_scope = toolz.merge(scope, pre_executed_scope)
    result = execute_until_in_scope(
        expr,
        new_scope,
        timecontext=timecontext,
        aggcontext=aggcontext,
        clients=clients,
        # XXX: we *explicitly* pass in scope and not new_scope here so that
        # post_execute sees the scope of execute_with_scope, not the scope of
        # execute_until_in_scope
        post_execute_=functools.partial(
            post_execute,
            scope=scope,
            timecontext=timecontext,
            aggcontext=aggcontext,
            clients=clients,
            **kwargs,
        ),
        **kwargs,
    )[op]['value']
    return result


@trace
def execute_until_in_scope(
    expr,
    scope,
    timecontext: Optional[TimeContext] = None,
    aggcontext=None,
    clients=None,
    post_execute_=None,
    **kwargs,
):
    """Execute until our op is in `scope`.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
    scope : Mapping
    timecontext : Optional[TimeContext]
    aggcontext : Optional[AggregationContext]
    clients : List[ibis.client.Client]
    kwargs : Mapping
    """
    # these should never be None
    assert aggcontext is not None, 'aggcontext is None'
    assert clients is not None, 'clients is None'
    assert post_execute_ is not None, 'post_execute_ is None'

    # base case: our op has been computed (or is a leaf data node), so
    # return the corresponding value
    op = expr.op()
    if isinstance(op, ops.Literal):
        # special case literals to avoid the overhead of dispatching
        # execute_node
        return scope_item(
            op,
            execute_literal(
                op, op.value, expr.type(), aggcontext=aggcontext, **kwargs
            ),
            timecontext,
        )

    if get_scope(scope, op, timecontext) is not None:
        return scope
    # figure out what arguments we're able to compute on based on the
    # expressions inputs. things like expressions, None, and scalar types are
    # computable whereas ``list``s are not
    computable_args = [arg for arg in op.inputs if is_computable_input(arg)]

    # pre_executed_states is a list of states with same the length of
    # computable_args, these states are passed to each arg
    if timecontext:
        arg_timecontexts = compute_time_context(
            op, num_args=len(computable_args), timecontext=timecontext
        )
    else:
        arg_timecontexts = [None] * len(computable_args)

    pre_executed_scope = pre_execute(
        op,
        *clients,
        scope=scope,
        timecontext=timecontext,
        aggcontext=aggcontext,
        **kwargs,
    )
    new_scope = toolz.merge(scope, pre_executed_scope)

    # Short circuit: if pre_execute puts op in scope, then we don't need to
    # execute its computable_args
    if get_scope(new_scope, op, timecontext) is not None:
        return new_scope

    # if op in new_scope:
    #    return new_scope

    # recursively compute each node's arguments until we've changed type.
    # compute_time_context should return with a list with the same length
    # as computable_args, the two lists will be zipping together for
    # further execution
    if len(arg_timecontexts) != len(computable_args):
        raise com.IbisError(
            'arg_timecontexts differ with computable_arg in length '
            f'for type:\n{type(op).__name__}.'
        )

    scopes = [
        execute_until_in_scope(
            arg,
            new_scope,
            timecontext=timecontext,
            aggcontext=aggcontext,
            post_execute_=post_execute_,
            clients=clients,
            **kwargs,
        )
        if hasattr(arg, 'op')
        else scope_item(arg, arg, timecontext)
        for (arg, timecontext) in zip(computable_args, arg_timecontexts)
    ]

    # if we're unable to find data then raise an exception
    if not scopes and computable_args:
        raise com.UnboundExpressionError(
            'Unable to find data for expression:\n{}'.format(repr(expr))
        )

    # there should be exactly one dictionary per computable argument
    assert len(computable_args) == len(scopes)

    new_scope = toolz.merge(new_scope, *scopes)

    # pass our computed arguments to this node's execute_node implementation
    data = [
        get_scope(new_scope, arg.op()) if hasattr(arg, 'op') else arg
        for arg in computable_args
    ]
    result = execute_node(
        op,
        *data,
        scope=scope,
        timecontext=timecontext,
        aggcontext=aggcontext,
        clients=clients,
        **kwargs,
    )
    computed = post_execute_(op, result, timecontext=timecontext)
    return scope_item(op, computed, timecontext)


execute = Dispatcher('execute')


@execute.register(ir.Expr)
@trace
def main_execute(
    expr,
    params=None,
    scope=None,
    timecontext: Optional[TimeContext] = None,
    aggcontext=None,
    **kwargs,
):
    """Execute an expression against data that are bound to it. If no data
    are bound, raise an Exception.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        The expression to execute
    params : Mapping[ibis.expr.types.Expr, object]
        The data that an unbound parameter in `expr` maps to
    scope : Mapping[ibis.expr.operations.Node, object]
        Additional scope, mapping ibis operations to data
    timecontext : Optional[TimeContext]
        timecontext needed for execution
    aggcontext : Optional[ibis.pandas.aggcontext.AggregationContext]
        An object indicating how to compute aggregations. For example,
        a rolling mean needs to be computed differently than the mean of a
        column.
    kwargs : Dict[str, object]
        Additional arguments that can potentially be used by individual node
        execution

    Returns
    -------
    result : Union[
        pandas.Series, pandas.DataFrame, ibis.pandas.core.simple_types
    ]

    Raises
    ------
    ValueError
        * If no data are bound to the input expression
    """
    if scope is None:
        scope = {}

    if timecontext is not None:
        # convert timecontext to datetime type, if time strings are provided
        timecontext = canonicalize_context(timecontext)

    if params is None:
        params = {}

    # TODO: make expresions hashable so that we can get rid of these .op()
    # calls everywhere
    additional_scope = {}
    for k, v in params.items():
        if hasattr(k, 'op'):
            item = scope_item(k.op(), v, timecontext)
        else:
            item = scope_item(k, v, timecontext)
        additional_scope = toolz.merge(additional_scope, item)

    new_scope = toolz.merge(scope, additional_scope)
    return execute_with_scope(
        expr,
        new_scope,
        timecontext=timecontext,
        aggcontext=aggcontext,
        **kwargs,
    )


def execute_and_reset(
    expr,
    params=None,
    scope=None,
    timecontext: Optional[TimeContext] = None,
    aggcontext=None,
    **kwargs,
):
    """Execute an expression against data that are bound to it. If no data
    are bound, raise an Exception.

    Notes
    -----
    The difference between this function and :func:`~ibis.pandas.core.execute`
    is that this function resets the index of the result, if the result has
    an index.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        The expression to execute
    params : Mapping[ibis.expr.types.Expr, object]
        The data that an unbound parameter in `expr` maps to
    scope : Mapping[ibis.expr.operations.Node, object]
        Additional scope, mapping ibis operations to data
    timecontext : Optional[TimeContext]
        timecontext needed for execution
    aggcontext : Optional[ibis.pandas.aggcontext.AggregationContext]
        An object indicating how to compute aggregations. For example,
        a rolling mean needs to be computed differently than the mean of a
        column.
    kwargs : Dict[str, object]
        Additional arguments that can potentially be used by individual node
        execution

    Returns
    -------
    result : Union[
        pandas.Series, pandas.DataFrame, ibis.pandas.core.simple_types
    ]

    Raises
    ------
    ValueError
        * If no data are bound to the input expression
    """
    result = execute(
        expr,
        params=params,
        scope=scope,
        timecontext=timecontext,
        aggcontext=aggcontext,
        **kwargs,
    )
    if isinstance(result, pd.DataFrame):
        schema = expr.schema()
        df = result.reset_index()
        return df.loc[:, schema.names]
    elif isinstance(result, pd.Series):
        return result.reset_index(drop=True)
    return result


compute_time_context = Dispatcher(
    'compute_time_context',
    doc="""\

Compute time context for a node in execution

Notes
-----
For a given node, return with a list of timecontext that are going to be
passed to its children nodes.
time context is useful when data is not uniquely defined by op tree. e.g.
a TableExpr can represent the query select count(a) from table, but the
result of that is different with time context (pd.Timestamp("20190101"),
pd.Timestamp("20200101")) vs (pd.Timestamp("20200101"),
pd.Timestamp("20210101“)), because what data is in "table" also depends on
the time context. And such context may not be global for all nodes. Each
node may have its own context. compute_time_context computes attributes that
are going to be used in executeion and passes these attributes to children
nodes.

Param:
timecontext : Optional[TimeContext]
    begin and end time context needed for execution

Return:
List[Optional[TimeContext]]
A list of timecontexts for children nodes of the current node. Note that
timecontext are calculated for children nodes of computable args only.
The length of the return list is same of the length of computable inputs.
See ``computable_args`` in ``execute_until_in_scope``
""",
)


@compute_time_context.register(ops.Node)
def compute_time_context_default(
    node, timecontext: Optional[TimeContext], **kwargs
):
    return [timecontext for arg in node.inputs if is_computable_input(arg)]


# In order to use time context feature, there must be a column of Timestamp
# type, and named as 'time' in TableExpr. This TIME_COL constant will be
# used in filtering data from a table or columns of a table.
TIME_COL = 'time'


def canonicalize_context(
    timecontext: Optional[TimeContext],
) -> Optional[TimeContext]:
    """Convert a timecontext to canonical one with type pandas.Timestamp
       for its begin and end time. Raise Exception for illegal inputs
    """
    SUPPORTS_TIMESTAMP_TYPE = pd.Timestamp
    try:
        begin, end = timecontext
    except (ValueError, TypeError):
        raise com.IbisError(
            f'Timecontext {timecontext} should specify (begin, end)'
        )

    if not isinstance(begin, SUPPORTS_TIMESTAMP_TYPE):
        raise com.IbisError(
            f'begin time value {begin} of type {type(begin)} is not'
            ' of type pd.Timestamp'
        )
    if not isinstance(end, SUPPORTS_TIMESTAMP_TYPE):
        raise com.IbisError(
            f'end time value {end} of type {type(begin)} is not'
            ' of type pd.Timestamp'
        )
    if begin > end:
        raise com.IbisError(
            f'begin time {begin} must be before or equal' f' to end time {end}'
        )
    return begin, end


class TimeContextRelation(enum.Enum):
    """ Enum to classify the relationship between two time contexts
        Assume that we have two timecontext c1 (begin1, end1),
        c2(begin2, end2):
        SUBSET means c1 is a subset of c2, begin1 is greater than or equal to
        begin2, and end1 is less than or equal to end2.
        Likewise, SUPERSET means that begin1 is earlier than begin2, and end1
        is later than end2.
        If neither of the two contexts is a superset of each other, and they
        share some time range in common, we called them OVERLAP.
        And NONOVERLAP means the two contexts doesn't overlap at all, which
        means end1 is earlier than begin2 or end2 is earlier than begin1
    """

    SUBSET = 0
    SUPERSET = 1
    OVERLAP = 2
    NONOVERLAP = 3


def compare_timecontext(cur_context: TimeContext, old_context: TimeContext):
    """Compare two timecontext and return the relationship between two time
    context (SUBSET, SUPERSET, OVERLAP, NONOVERLAP).

    Parameters
    ----------
    cur_context: TimeContext
    old_context: TimeContext

    Returns
    -------
    result : Enum[TimeContextRelation]
    """
    begin, end = cur_context
    old_begin, old_end = old_context
    if old_begin <= begin and old_end >= end:
        return TimeContextRelation.SUBSET
    elif old_begin >= begin and old_end <= end:
        return TimeContextRelation.SUPERSET
    elif old_end < begin or end < old_begin:
        return TimeContextRelation.NONOVERLAP
    else:
        return TimeContextRelation.OVERLAP


def scope_item(op, result, timecontext):
    """make a scope item to be set in scope

    Scope is a dictionary mapping :class:`~ibis.expr.operations.Node`
    subclass instances to concrete data, and the time context associate
    with it(if any).

    Parameters
    ----------
    op: ibis.expr.operations.Node, key in scope.
    result : scalar, pd.Series, pd.DataFrame, concrete data.
    timecontext: Optional[TimeContext], time context associate with the
    result.

    Returns
    -------
    scope_item : Dict, a key value pair that could merge into scope later
    """
    return {op: {'value': result, 'timecontext': timecontext}}


def get_scope(scope, op, timecontext=None):
    """ Given a op and timecontext, get result from scope
    Parameters
    ----------
    scope: collections.Mapping
    a dictionary mapping :class:`~ibis.expr.operations.Node`
    subclass instances to concrete data, and the time context associate
    with it(if any).
    op: ibis.expr.operations.Node, key in scope.
    timecontext: Optional[TimeContext]

    Returns
    -------
    result : scalar, pd.Series, pd.DataFrame
    """
    if op not in scope:
        return None
    # for ops without timecontext
    if timecontext is None:
        return scope[op]['value']
    else:
        # For op with timecontext, we only use scope to cache leaf nodes for
        # now. This is because some ops cannot use cached result with a
        # different timecontext to get the correct result. For example,
        # a groupby followed by count, if we use a larger or smaller dataset
        # from cache, we will probably get an error in result. Such ops with
        # global aggregation, ops whose result is depending on other rows
        # in result Dataframe, cannot use cached result with different time
        # context to optimize calculation. For simplicity, we skip these ops
        # for now.
        if isinstance(op, ops.TableColumn) or isinstance(op, ops.TableNode):
            relation = compare_timecontext(
                timecontext, scope[op]['timecontext']
            )
            if relation == TimeContextRelation.SUBSET:
                return scope[op]['value']
            else:
                return None
        else:
            # For other ops with time context,  do not trust results in scope,
            # return None as if result is not present
            return None
