import os

from universalis.common.operator import StatefulFunction, Operator

sink_operator = Operator('sink')


@sink_operator.register
async def output(ctx: StatefulFunction, *args):
    return args
