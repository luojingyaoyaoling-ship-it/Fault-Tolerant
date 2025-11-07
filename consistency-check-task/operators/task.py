import os

from universalis.common.operator import StatefulFunction, Operator

task_operator = Operator('task')


@task_operator.register
async def executeTask(ctx: StatefulFunction, *args):
    operators = list(args[0])
    key = operators.index(task_operator.name)
    result = task(args)
    await ctx.call_remote_function_no_response(operator_name='sink',
                                               function_name='output',
                                               key=key,
                                               params=(result, ))


def task(args):
    import subprocess
    base_path = "/usr/local/universalis"
    script_path = "./self_task/test.sh"
    final_path = os.path.join(base_path, script_path)
    result = subprocess.run(["bash", final_path, args[1]["file1"], args[1]["file2"]], capture_output=True, text=True, check=False,)
    return result.stdout.strip()
