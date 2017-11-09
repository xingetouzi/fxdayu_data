# encoding:utf-8
import click
from fxdayu_data import DataConfig


@click.group()
def config():
    pass


@config.command()
@click.option("--name", '-n', default=DataConfig.DEFAULT)
@click.option("--copy", "-c", is_flag=True, flag_value=True, default=False)
@click.argument("path", nargs=1)
def add(name, path, copy):
    """Add a config path into DataAPI"""
    if copy:
        import shutil
        path = shutil.copy(path, DataConfig.get_root())

    import os
    path = os.path.abspath(path)

    paths = DataConfig.get_config_paths()
    paths[name] = path
    DataConfig.set_config_paths(paths)


@config.command()
@click.argument("name")
def use(name):
    """Find and Set a config path as the main path in DataAPI by its name"""
    paths = DataConfig.get_config_paths()
    paths[DataConfig.DEFAULT] = paths[name]
    DataConfig.set_config_paths(paths)


@config.command()
@click.argument("names", nargs=-1)
def delete(names):
    """Delete config paths by names"""
    paths = DataConfig.get_config_paths()
    for name in names:
        paths.pop(name)
    DataConfig.set_config_paths(paths)


@config.command()
def show():
    """Show all config paths"""
    for item in list(DataConfig.get_config_paths().items()):
        print("%s: %s" % item)


@config.command()
@click.argument("path", default="config.py")
@click.option("-t", "--type", default="mongo", required=False,
              help="Specify config type: mongo or bundle, default: mongo.")
@click.option("-n", "--name", default=None, required=False,
              help="Add this file to DataAPI with name input.")
def export(path, type, name):
    """Export default config"""
    from fxdayu_data import default
    import os

    if os.path.isdir(path):
        path = os.path.join(path, "config.py")

    with open(path, "w") as f:
        f.write(default.defaults.get(type, default.MONGOCONFIG))
        if name:
            add.callback(name, os.path.abspath(path))


@config.command("exec")
@click.argument("arguments", nargs=-1)
def execute(arguments):
    """
    Read data from DataAPI
    """

    from fxdayu_data import DataAPI

    func = arguments[0]

    def catch_values(values):
        if "," in values:
            return values.split(",")
        else:
            return values

    def separate_key(kv):
        key, value = kv.split("=")
        return key, catch_values(value)

    args = []
    kwargs = {}

    for arg in arguments[1:]:
        if "=" in arg:
            kwargs.__setitem__(*separate_key(arg))
        else:
            args.append(catch_values(arg))

    click.echo(DataAPI.get(func, *args, **kwargs))
