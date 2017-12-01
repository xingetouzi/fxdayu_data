# encoding:utf-8
import click
from fxdayu_data import config
from fxdayu_data.commands import *


OPTION_NAME = click.Option(["--name", '-n'], default=config.DEFAULT,
                           help="Name of this config file, default: %s" % config.DEFAULT)
OPTION_COPY = click.Option(["--copy", "-c"], is_flag=True, flag_value=True, default=False)
OPTION_TYPE = click.Option(["-t", "--type"], default="mongo", required=False,
                           help="Specify config type: mongo or bundle, default: mongo.")
OPTION_EXPORT_NAME = click.Option(["-n", "--name"], default=None, required=False,
                                  help="Add this file to DataAPI with name input.")
OPTION_NAME_BUNDLE = click.Option(["--name", '-n'], default="bundle",
                                  help="Name of its config file to set.")
OPTION_IGNORE = click.Option(["-i", "--ignore"], default=False, is_flag=True,
                             help="Do not use extracted data config as main.")


ARGS_PATH_REQUIRE = click.Argument(["path"], nargs=1)
ARGS_PATH = click.Argument(["path"], nargs=1, default="config.py")
ARGS_NAME = click.Argument(["name"], nargs=1)
ARGS_NAMES = click.Argument(["names"], nargs=-1)
ARGS_ARGUMENTS = click.Argument(["arguments"], nargs=-1)
ARGS_SOURCE = click.Argument(['source'], nargs=1)
ARGS_TARGET = click.Argument(['target'], nargs=1, default=config.default_bundle_path())


api = click.Group(
    "DataAPI",
    commands={
        "add": click.Command("add", callback=add, params=[OPTION_NAME, ARGS_PATH_REQUIRE, OPTION_COPY],
                             short_help="""Add a config path into DataAPI."""),
        "use": click.Command("use", callback=use, params=[ARGS_NAME],
                             short_help="""Find and Set a config path as the main path in DataAPI by its name."""),
        "delete": click.Command("delete", callback=delete, params=[ARGS_NAMES],
                                short_help="""Delete config paths by names."""),
        "show": click.Command("show", callback=show, short_help="Show current config files."),
        "export": click.Command("export", callback=export, short_help="Export default config file.",
                                params=[ARGS_PATH, OPTION_TYPE, OPTION_EXPORT_NAME, OPTION_COPY]),
        "exec": click.Command("exec", callback=execute, params=[ARGS_ARGUMENTS],
                              short_help="Execute DataAPI functions."),
        "extract": click.Command("extract", callback=extract, short_help="Extract compressed file as data.",
                                 params=[OPTION_NAME_BUNDLE, OPTION_IGNORE, ARGS_SOURCE, ARGS_TARGET])
    }
)

if __name__ == '__main__':
    import sys

    sys.argv.extend("extract D:/WorkingArea/bundle.2017-12-01.tar.gz".split(" "))
    api()