import os

from universalis.common.operator import StatefulFunction, Operator

source_operator = Operator('source')


@source_operator.register
async def read(ctx: StatefulFunction, *args):
    operators = list(args[0])
    key = operators.index(source_operator.name)
    await ctx.call_remote_function_no_response(operator_name='task',
                                                function_name='executeTask',
                                                key=key,
                                                params=args)


