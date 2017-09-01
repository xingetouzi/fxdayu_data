import click
from fxdayu_data import DataConfig


@click.group()
def config():
    pass


@config.command()
@click.option("--name", '-n', default=DataConfig.DEFAULT)
@click.option("--path", '-p')
def add(name, path):
    """Add a config path into DataAPI"""
    if path is None:
        path = name

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
def export(path):
    """Export default config"""
    from fxdayu_data.default import config
    import os

    if os.path.isdir(path):
        path = os.path.join(path, "config.py")

    with open(path, "w") as f:
        f.write(config)

